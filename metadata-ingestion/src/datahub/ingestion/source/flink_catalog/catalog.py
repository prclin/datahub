from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import Catalog


class FlinkHiveCatalog(Catalog):
    """
    A catalog implementation for Hive, and use literal hive conf value.
    """

    def __init__(
        self,
        catalog_name: str,
        default_database: str = None,
        hive_conf_dict: dict[str, str] = None,
        hive_version: str = None,
    ):
        assert catalog_name is not None

        gateway = get_gateway()
        # construct HiveConf
        hive_conf = gateway.jvm.org.apache.hadoop.hive.conf.HiveConf()
        for key, value in hive_conf_dict:
            hive_conf.set(key, value)
        j_hive_catalog = gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
            catalog_name, default_database, hive_conf, hive_version
        )
        super().__init__(j_hive_catalog)
