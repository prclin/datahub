from typing import Optional

from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import Catalog


class FlinkHiveCatalog(Catalog):
    """
    A catalog implementation for Hive, and use literal hive conf value.
    """

    def __init__(
        self,
        catalog_name: str,
        default_database: Optional[str] = None,
        hive_version: Optional[str] = None,
        hive_conf_dir: Optional[str] = None,
        hive_conf_dict: Optional[dict[str, str]] = None,
    ):
        assert catalog_name is not None
        if not hive_conf_dir and not hive_conf_dict:
            raise ValueError(
                "should specify at least one of hive conf and hive conf dir!"
            )
        gateway = get_gateway()
        # construct HiveConf

        hive_conf = gateway.jvm.org.apache.hadoop.hive.conf.HiveConf()
        if hive_conf_dir:
            hive_conf.addResource(hive_conf_dir)
        if hive_conf_dict:
            for key, value in hive_conf_dict:
                hive_conf.set(key, value)
        j_hive_catalog = gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
            catalog_name, default_database, hive_conf, hive_version
        )
        super().__init__(j_hive_catalog)
