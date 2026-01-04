import argparse
import getpass
import glob
import importlib
import logging
import os
import platform
import shlex
import shutil
import signal
import socket
import struct
import sys
import tempfile
import time
from collections import namedtuple
from logging import WARN
from multiprocessing import RLock
from string import Template
from subprocess import PIPE, Popen

import py4j
from py4j.java_gateway import (
    CallbackServerParameters,
    GatewayParameters,
    JavaGateway,
    logger,
)

from datahub.ingestion.source.flink_catalog.exceptions import install_exception_handler

_gateway = None
_lock = RLock()


def get_gateway() -> JavaGateway:
    global _gateway
    global _lock
    with _lock:
        if _gateway is None:
            # Set the level to WARN to mute the noisy INFO level logs
            logger.level = WARN
            # if Java Gateway is already running
            if "PYFLINK_GATEWAY_PORT" in os.environ:
                gateway_port = int(os.environ["PYFLINK_GATEWAY_PORT"])
                gateway_param = GatewayParameters(port=gateway_port, auto_convert=True)
                _gateway = JavaGateway(
                    gateway_parameters=gateway_param,
                    callback_server_parameters=CallbackServerParameters(
                        port=0, daemonize=True, daemonize_connections=True
                    ),
                )
            else:
                _gateway = launch_gateway()

            callback_server = _gateway.get_callback_server()
            callback_server_listening_address = callback_server.get_listening_address()
            callback_server_listening_port = callback_server.get_listening_port()
            _gateway.jvm.org.apache.flink.client.python.PythonEnvUtils.resetCallbackClient(
                _gateway.java_gateway_server,
                callback_server_listening_address,
                callback_server_listening_port,
            )
            install_exception_handler()
            install_py4j_hooks()
            _gateway.entry_point.put("PythonFunctionFactory", PythonFunctionFactory())
            _gateway.entry_point.put("Watchdog", Watchdog())
    return _gateway


class Watchdog(object):
    """
    Used to provide to Java side to check whether its parent process is alive.
    """

    def ping(self):
        time.sleep(10)
        return True

    class Java:
        implements = ["org.apache.flink.client.python.PythonGatewayServer$Watchdog"]


class PythonFunctionFactory(object):
    """
    Used to create PythonFunction objects for Java jobs.
    """

    def getPythonFunction(self, moduleName, objectName):
        udf_wrapper = getattr(importlib.import_module(moduleName), objectName)
        return udf_wrapper._java_user_defined_function()

    class Java:
        implements = ["org.apache.flink.client.python.PythonFunctionFactory"]


def install_py4j_hooks():
    """
    Hook the classes such as JavaPackage, etc of Py4j to improve the exception message.
    """

    def wrapped_call(self, *args, **kwargs):
        raise TypeError(
            "Could not found the Java class '%s'. The Java dependencies could be specified via "
            "gradle dependencies" % self._fqn
        )

    py4j.java_gateway.JavaPackage.__call__ = wrapped_call


def launch_gateway() -> JavaGateway:
    """
    launch jvm gateway
    """
    args = ["-c", "org.apache.flink.client.python.PythonGatewayServer"]

    submit_args = os.environ.get("SUBMIT_ARGS", "local")
    args += shlex.split(submit_args)

    # Create a temporary directory where the gateway server should write the connection information.
    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        _find_flink_home()

        env = dict(os.environ)
        env["_PYFLINK_CONN_INFO_PATH"] = conn_info_file

        p = launch_gateway_server_process(env, args)

        while not p.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            stderr_info = p.stderr.read().decode("utf-8")
            raise RuntimeError(
                "Java gateway process exited before sending its port number.\nStderr:\n"
                + stderr_info
            )

        with open(conn_info_file, "rb") as info:
            gateway_port = struct.unpack("!I", info.read(4))[0]
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auto_convert=True),
        callback_server_parameters=CallbackServerParameters(
            port=0, daemonize=True, daemonize_connections=True
        ),
    )

    return gateway


def launch_gateway_server_process(env, args):
    prepare_environment_variables(env)
    program_args = construct_program_args(args)
    if program_args.cluster_type == "local":
        java_executable = find_java_executable()
        log_settings = construct_log_settings(env)
        jvm_args = env.get("JVM_ARGS", "").split()
        jvm_opts = get_jvm_opts(env)
        classpath = os.pathsep.join([construct_flink_classpath(env)])
        command = [
            java_executable,
            *jvm_args,
            "-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=jdk.proxy2/jdk.proxy2=ALL-UNNAMED",
            *jvm_opts,
            *log_settings,
            "-cp",
            classpath,
            program_args.main_class,
            *program_args.other_args,
        ]
    else:
        command = [
            os.path.join(env["FLINK_BIN_DIR"], "flink"),
            "run",
            *program_args.other_args,
            "-c",
            program_args.main_class,
        ]
    preexec_fn = None
    if not on_windows():

        def preexec_func():
            # ignore ctrl-c / SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        preexec_fn = preexec_func
    return Popen(
        list(filter(lambda c: len(c) != 0, command)),
        stdin=PIPE,
        stderr=PIPE,
        preexec_fn=preexec_fn,
        env=env,
    )


def construct_flink_classpath(env):
    _find_flink_home()
    flink_lib_directory = env["FLINK_LIB_DIR"]

    if on_windows():
        # The command length is limited on Windows. To avoid the problem we should shorten the
        # command length as much as possible.
        lib_jars = os.path.join(flink_lib_directory, "*")
    else:
        lib_jars = os.pathsep.join(
            glob.glob(os.path.join(flink_lib_directory, "*.jar"))
        )

    return os.pathsep.join([lib_jars])


def get_jvm_opts(env):
    jvm_opts = env.get("FLINK_ENV_JAVA_OPTS")
    if jvm_opts is None:
        default_jvm_opts = read_from_config(
            KEY_ENV_JAVA_DEFAULT_OPTS, "", env["FLINK_CONF_DIR"]
        )
        extra_jvm_opts = read_from_config(
            KEY_ENV_JAVA_OPTS,
            read_from_config(KEY_ENV_JAVA_OPTS_DEPRECATED, "", env["FLINK_CONF_DIR"]),
            env["FLINK_CONF_DIR"],
        )
        jvm_opts = default_jvm_opts + " " + extra_jvm_opts

    # Remove leading and trailing double quotes (if present) of value
    jvm_opts = jvm_opts.strip('"')
    return jvm_opts.split()


def construct_log_settings(env):
    templates = [
        "-Dlog.file=${flink_log_dir}/flink-${flink_ident_string}-python-${hostname}.log",
        "-Dlog4j.configuration=${log4j_properties}",
        "-Dlog4j.configurationFile=${log4j_properties}",
        "-Dlogback.configurationFile=${logback_xml}",
    ]

    flink_home = os.path.realpath(_find_flink_home())
    flink_conf_dir = env["FLINK_CONF_DIR"]

    if "FLINK_LOG_DIR" in env:
        flink_log_dir = env["FLINK_LOG_DIR"]
    else:
        flink_log_dir = read_from_config(
            KEY_ENV_LOG_DIR, os.path.join(flink_home, "log"), env["FLINK_CONF_DIR"]
        )

    if "LOG4J_PROPERTIES" in env:
        log4j_properties = env["LOG4J_PROPERTIES"]
    else:
        log4j_properties = "%s/log4j-cli.properties" % flink_conf_dir

    if "LOGBACK_XML" in env:
        logback_xml = env["LOGBACK_XML"]
    else:
        logback_xml = "%s/logback.xml" % flink_conf_dir

    if "FLINK_IDENT_STRING" in env:
        flink_ident_string = env["FLINK_IDENT_STRING"]
    else:
        flink_ident_string = getpass.getuser()

    hostname = socket.gethostname()
    log_settings = []
    for template in templates:
        log_settings.append(
            Template(template).substitute(
                log4j_properties=log4j_properties,
                logback_xml=logback_xml,
                flink_log_dir=flink_log_dir,
                flink_ident_string=flink_ident_string,
                hostname=hostname,
            )
        )
    return log_settings


def prepare_environment_variables(env):
    flink_home = _find_flink_home()
    # get the realpath of tainted path value to avoid CWE22 problem that constructs a path or URI
    # using the tainted value and might allow an attacker to access, modify, or test the existence
    # of critical or sensitive files.
    real_flink_home = os.path.realpath(flink_home)

    if "FLINK_CONF_DIR" in env:
        flink_conf_directory = os.path.realpath(env["FLINK_CONF_DIR"])
    else:
        flink_conf_directory = os.path.join(real_flink_home, "conf")
    env["FLINK_CONF_DIR"] = flink_conf_directory

    if "FLINK_LIB_DIR" in env:
        flink_lib_directory = os.path.realpath(env["FLINK_LIB_DIR"])
    else:
        flink_lib_directory = os.path.join(real_flink_home, "lib")
    env["FLINK_LIB_DIR"] = flink_lib_directory

    if "FLINK_OPT_DIR" in env:
        flink_opt_directory = os.path.realpath(env["FLINK_OPT_DIR"])
    else:
        flink_opt_directory = os.path.join(real_flink_home, "opt")
    env["FLINK_OPT_DIR"] = flink_opt_directory

    if "FLINK_PLUGINS_DIR" in env:
        flink_plugins_directory = os.path.realpath(env["FLINK_PLUGINS_DIR"])
    else:
        flink_plugins_directory = os.path.join(real_flink_home, "plugins")
    env["FLINK_PLUGINS_DIR"] = flink_plugins_directory

    env["FLINK_BIN_DIR"] = os.path.join(real_flink_home, "bin")


def construct_program_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--class", required=True)
    parser.add_argument("cluster_type", choices=["local", "remote", "yarn"])
    parse_result, other_args = parser.parse_known_args(args)
    main_class = getattr(parse_result, "class")
    cluster_type = parse_result.cluster_type
    return namedtuple("ProgramArgs", ["main_class", "cluster_type", "other_args"])(
        main_class, cluster_type, other_args
    )


KEY_ENV_LOG_DIR = "env.log.dir"
KEY_ENV_YARN_CONF_DIR = "env.yarn.conf.dir"
KEY_ENV_HADOOP_CONF_DIR = "env.hadoop.conf.dir"
KEY_ENV_HBASE_CONF_DIR = "env.hbase.conf.dir"
KEY_ENV_JAVA_HOME = "env.java.home"
KEY_ENV_JAVA_OPTS = "env.java.opts.all"
KEY_ENV_JAVA_OPTS_DEPRECATED = "env.java.opts"
KEY_ENV_JAVA_DEFAULT_OPTS = "env.java.default-opts.all"


def find_java_executable():
    java_executable = "java.exe" if on_windows() else "java"
    flink_home = _find_flink_home()
    flink_conf_directory = os.path.join(flink_home, "conf")
    java_home = read_from_config(KEY_ENV_JAVA_HOME, None, flink_conf_directory)

    if java_home is None and "JAVA_HOME" in os.environ:
        java_home = os.environ["JAVA_HOME"]

    if java_home is not None:
        java_executable = os.path.join(java_home, "bin", java_executable)

    return java_executable


def read_from_config(key, default_value, flink_conf_directory):
    from ruamel.yaml import YAML

    yaml = YAML(typ="safe")
    # try to find flink-conf.yaml file in flink_conf_directory
    flink_conf_file = os.path.join(flink_conf_directory, "flink-conf.yaml")
    if os.path.isfile(flink_conf_file):
        # If flink-conf.yaml exists, use the old parsing logic to read the value
        # get the realpath of tainted path value to avoid CWE22 problem that constructs a path
        # or URI using the tainted value and might allow an attacker to access, modify, or test
        # the existence of critical or sensitive files.
        with open(os.path.realpath(flink_conf_file), "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                if line.startswith("#") or len(line.strip()) == 0:
                    continue
                k, v = line.split(":", 1)
                if k.strip() == key:
                    return v.strip()
    else:
        # If flink-conf.yaml does not exist, try to find config.yaml instead
        config_file = os.path.join(flink_conf_directory, "config.yaml")
        if os.path.isfile(config_file):
            # If config.yaml exists, use YAML parser to read the value
            with open(os.path.realpath(config_file), "r") as f:
                config = yaml.load(f)
                flat_config = flatten_config(config)
                return flat_config.get(key, default_value)

    # If neither file exists, return the default value
    return default_value


def flatten_config(config, parent_key=""):
    items = []
    sep = "."
    for k, v in config.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_config(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)


def on_windows():
    return platform.system() == "Windows"


def _find_flink_home():
    """
    Find the FLINK_HOME.
    """
    # If the environment has set FLINK_HOME, trust it.
    if "FLINK_HOME" in os.environ:
        return os.environ["FLINK_HOME"]

    try:
        current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
        os.environ["FLINK_LIB_DIR"] = os.path.join(current_dir, "lib")
        return current_dir
    except Exception:
        pass
    logging.error(
        "Could not find valid FLINK_HOME(Flink distribution directory) "
        "in current environment."
    )
    sys.exit(-1)
