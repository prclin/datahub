from typing import Optional, Union

from py4j.java_gateway import JavaObject

from datahub.ingestion.source.flink_catalog.java_gateway import get_gateway
from datahub.metadata._internal_schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)


class CatalogDatabase:
    def __init__(self, j_database: JavaObject):
        self.j_database = j_database

    def get_comment(self) -> str:
        return self.j_database.getComment()

    def get_properties(self) -> dict[str, str]:
        return self.j_database.getProperties()


def get_data_type_factory(jvm) -> JavaObject:
    constructors = jvm.Class.forName(
        "org.apache.flink.table.catalog.DataTypeFactoryImpl"
    ).getDeclaredConstructors()
    constructor = None
    for constructor in constructors:
        if constructor.getParameterTypes().length == 3:
            break
    if not constructor:
        raise Exception(
            "org.apache.flink.table.catalog.DataTypeFactoryImpl does not exist!"
        )

    classloader = jvm.Thread.currentThread().getContextClassLoader()
    config = jvm.org.apache.flink.table.api.TableConfig.getDefault()
    serializer_config = (
        jvm.org.apache.flink.api.common.serialization.SerializerConfigImpl()
    )
    clazz = jvm.Class.forName("java.lang.Class")
    parameter_classes = jvm.java.lang.reflect.Array.newInstance(clazz, 3)
    parameter_classes[0] = classloader.getClass()
    parameter_classes[1] = config.getClass()
    parameter_classes[2] = serializer_config.getClass()
    invoker = jvm.org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.buildInvoker(
        constructor, parameter_classes
    )

    obj = jvm.Class.forName("java.lang.Object")
    parameters = jvm.java.lang.reflect.Array.newInstance(obj, 3)
    parameters[0] = classloader
    parameters[1] = config
    parameters[2] = serializer_config
    r_engine = jvm.org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine()
    data_type_factory = r_engine.invoke(None, invoker, parameters)

    return data_type_factory


class Column:
    __jvm = get_gateway().jvm
    data_type_factory = get_data_type_factory(__jvm)

    def __init__(self, j_column: JavaObject):
        self.j_column = j_column
        column_type = (
            j_column.getDataType().toDataType(self.data_type_factory).getLogicalType()
        )
        self.column_type = ColumnType(column_type)
        self.name = j_column.getName()
        self.comment = str(j_column.getComment().orElse(None))


TypeClasses = Union[
    "BooleanTypeClass",
    "FixedTypeClass",
    "StringTypeClass",
    "BytesTypeClass",
    "NumberTypeClass",
    "DateTypeClass",
    "TimeTypeClass",
    "EnumTypeClass",
    "NullTypeClass",
    "RecordTypeClass",
    "MapTypeClass",
    "ArrayTypeClass",
    "UnionTypeClass",
]


class ColumnType:
    TYPE_MAP: dict[str, TypeClasses] = {
        "CHAR": StringTypeClass,
        "VARCHAR": StringTypeClass,
        "BOOLEAN": BooleanTypeClass,
        "BINARY": BytesTypeClass,
        "VARBINARY": BytesTypeClass,
        "DECIMAL": NumberTypeClass,
        "TINYINT": NumberTypeClass,
        "SMALLINT": NumberTypeClass,
        "INTEGER": NumberTypeClass,
        "BIGINT": NumberTypeClass,
        "FLOAT": NumberTypeClass,
        "DOUBLE": NumberTypeClass,
        "DATE": DateTypeClass,
        "TIME_WITHOUT_TIME_ZONE": TimeTypeClass,
        "TIMESTAMP_WITHOUT_TIME_ZONE": TimeTypeClass,
        "TIMESTAMP_WITH_TIME_ZONE": TimeTypeClass,
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE": TimeTypeClass,
        "INTERVAL_YEAR_MONTH": TimeTypeClass,
        "INTERVAL_DAY_TIME": TimeTypeClass,
        "ARRAY": ArrayTypeClass,
        "MULTISET": MapTypeClass,
        "MAP": MapTypeClass,
        "ROW": RecordTypeClass,
        "DISTINCT_TYPE": UnionTypeClass,
        "STRUCTURED_TYPE": UnionTypeClass,
        "NULL": NullTypeClass,
        "RAW": FixedTypeClass,
        "SYMBOL": EnumTypeClass,
        "UNRESOLVED": UnionTypeClass,
    }
    __jvm = get_gateway().jvm
    native_data_type: str

    def __init__(self, j_column_type: JavaObject):
        self.j_column_type = j_column_type
        # short type like MAP,VARCHAR ...
        type_name = j_column_type.getTypeRoot().name()
        # long type like MAP<STRING,INT>,ARRAY<STRING> ...
        self.native_data_type = j_column_type.toString()
        self.nullable = j_column_type.isNullable()

        self.fields: Optional[list[tuple[str, str | None, ColumnType]]] = None

        # process subtypes
        type_class = self.TYPE_MAP.get(type_name, StringTypeClass)
        match type_class:
            case _ if type_class is ArrayTypeClass:
                element_type = ColumnType(j_column_type.getElementType())
                self.type_class = type_class([element_type.native_data_type])
            case _ if type_class is MapTypeClass:
                # multiset represents liken MAP<ANY,INT>
                key_type = ColumnType(
                    j_column_type.getElementType()
                    if type_name == "MULTISET"
                    else j_column_type.getKeyType()
                )
                value_type = ColumnType(
                    self.__jvm.org.apache.flink.table.types.logical.IntType(False)
                    if type_name == "MULTISET"
                    else j_column_type.getValueType()
                )
                self.type_class = type_class(
                    key_type.native_data_type, value_type.native_data_type
                )
            case _ if type_class is RecordTypeClass:
                fields = j_column_type.getFields()
                # (name,comment,type)
                self.fields = [
                    (
                        str(field.getName()),
                        field.getDescription().orElse(None),
                        ColumnType(field.getType()),
                    )
                    for field in fields
                ]
                self.type_class = type_class()
            case _:
                self.type_class = type_class()


class Schema:
    def __init__(self, j_schema: JavaObject):
        self.j_schema = j_schema

    def get_primary_keys(self) -> list[str]:
        primary_key = self.j_schema.getPrimaryKey().orElse(None)
        primary_keys = list(primary_key.getColumnNames()) if primary_key else []
        return primary_keys

    def get_watermark_spec(self) -> tuple[str, str]:
        specs = self.j_schema.getWatermarkSpecs()
        watermark_column = specs[0].getColumnName() if specs else None
        watermark_expression = specs[0].toString() if specs else None
        return watermark_column, watermark_expression

    def get_columns(self) -> list[Column]:
        return [Column(col) for col in self.j_schema.getColumns()]


class HiveTable:
    def __init__(self, j_table: JavaObject):
        self.j_table = j_table

    def get_location(self) -> str:
        return self.j_table.getSd().getLocation()


class CatalogTable:
    def __init__(self, j_table: JavaObject):
        self.j_table = j_table

    def get_table_kind(self) -> str:
        return self.j_table.getTableKind().name()

    def get_options(self) -> dict[str, str]:
        return dict(self.j_table.getOptions())

    def get_comment(self) -> str:
        return self.j_table.getComment()

    def get_unresolved_schema(self) -> Schema:
        return Schema(self.j_table.getUnresolvedSchema())

    def get_partition_keys(self) -> list[str]:
        return self.j_table.getPartitionKeys()

    def get_original_query(self) -> str:
        return self.j_table.getOriginalQuery()


class FlinkHiveCatalog:
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
            path = gateway.jvm.org.apache.hadoop.fs.Path(hive_conf_dir, "hive-site.xml")
            inputs = path.getFileSystem(hive_conf).open(path)
            hive_conf.addResource(inputs, path.toString())
        if hive_conf_dict:
            for key, value in hive_conf_dict:
                hive_conf.set(key, value)
        self.j_hive_catalog = (
            gateway.jvm.org.apache.flink.table.catalog.hive.HiveCatalog(
                catalog_name, default_database, hive_conf, hive_version
            )
        )

    def open(self):
        self.j_hive_catalog.open()

    def list_databases(self) -> list[str]:
        return self.j_hive_catalog.listDatabases()

    def list_tables(self, database: str) -> list[str]:
        return self.j_hive_catalog.listTables(database)

    def get_database(self, database: str) -> CatalogDatabase:
        return CatalogDatabase(self.j_hive_catalog.getDatabase(database))

    def get_hive_table(self, database: str, table: str) -> HiveTable:
        return HiveTable(
            self.j_hive_catalog.getHiveTable(gen_object_path(database, table))
        )

    def instantiate_catalog_table(self, hive_table: HiveTable) -> CatalogTable:
        catalog_table = self.j_hive_catalog.instantiateCatalogTable(hive_table.j_table)
        return CatalogTable(catalog_table)


def gen_object_path(database: str, table: str) -> JavaObject:
    gateway = get_gateway()
    return gateway.jvm.org.apache.flink.table.catalog.ObjectPath(database, table)
