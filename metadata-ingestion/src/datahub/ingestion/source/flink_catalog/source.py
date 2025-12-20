import json
from typing import Iterable, Optional, Union

from py4j.java_gateway import JavaObject
from pydantic import Field, ValidationError, model_validator
from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import ObjectPath

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    CatalogKey,
    DatabaseKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.flink_catalog.catalog import FlinkHiveCatalog
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata._internal_schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    GlobalTagsClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TagKeyClass,
    TimeTypeClass,
    UnionTypeClass,
)


class FlinkCatalogConfig(StatefulIngestionConfigBase):
    default_database: str = "default"
    name: str = "hive"

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="database patterns which need to sync.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="table patterns which need to sync.",
    )
    view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="view patterns which need to sync.",
    )

    hive_version: str = Field(
        default=None, description="hive version string like 3.1.3"
    )
    hive_conf_dir: str = Field(
        default=None,
        description="hive configuration directory,which contains the hive-site.xml.",
    )
    hive_conf: dict[str, str] = Field(default=None, description="hive configurations.")

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="stateful ingestion configs"
    )
    platform_instance: str = Field(
        default=None, description="platform instance identifier."
    )
    env: str = Field(
        default="PROD",
        description="The environment that all assets produced by this connector belong to.",
    )

    @model_validator(mode="after")
    def check_and_complete_conf(self):
        """
        validate the configuration
        in hive_catalog, one of hive_conf and hive_conf_dir should be not none.
        """
        if not self.hive_conf and not self.hive_conf_dir:
            raise ValidationError(
                "should specify at least one of hive conf and hive conf dir!"
            )
        return self


class FlinkCatalogReport(StatefulIngestionReport):
    pass


# todo 补全
TYPE_MAP: dict[
    str,
    Union[
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
    ],
] = {
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
    "MULTISET": ArrayTypeClass,
    "MAP": MapTypeClass,
    "ROW": RecordTypeClass,
    "DISTINCT_TYPE": UnionTypeClass,
    "STRUCTURED_TYPE": UnionTypeClass,
    "NULL": NullTypeClass,
    "RAW": FixedTypeClass,
    "SYMBOL": EnumTypeClass,
    "UNRESOLVED": UnionTypeClass,
}


class FlinkCatalogSource(metaclass=StatefulIngestionSourceBase):
    platform: str = "flink"
    watermark_tag_urn: str = make_tag_urn("flink.watermark")

    def __init__(self, config: FlinkCatalogConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.ctx = ctx
        self.report = FlinkCatalogReport()
        self.catalog = self.get_catalog()
        jvm = get_gateway().jvm
        self.data_type_factory = jvm.org.apache.flink.table.catalog.DataTypeFactoryImpl(
            jvm.Thread.currentThread().contextClassloader,
            jvm.org.apache.flink.table.api.TableConfig.getDefault(),
            None,
        )

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        # add flink watermark tag
        yield self.gen_watermark_tag()

        # database level
        databases = self.get_syncable_databases()
        for database in databases:
            db = self.catalog.get_database(database)
            description = db.get_comment()
            properties = db.get_properties()
            yield from self.gen_database_containers(database, description, properties)
            # table level
            tables = self.get_syncable_tables(database)
            for table in tables:
                dataset_urn = self.gen_dataset_urn(database, table)
                # to container
                yield from add_dataset_to_container(
                    self.gen_database_key(database), dataset_urn
                )
                # to platform instance
                dpi_aspect = self.add_dataset_to_dataplatform_instance(dataset_urn)
                if dpi_aspect:
                    yield dpi_aspect
                # status
                yield self.gen_dataset_status(dataset_urn)

                # get table
                hive_table = self.catalog._j_catalog.getHiveTable(
                    ObjectPath(database, table)._j_object_path
                )
                catalog_table = self.catalog._j_catalog.instantiateCatalogTable(
                    hive_table
                )

                # subtype
                table_kind = catalog_table.getTableKind().name()
                match table_kind:
                    case "VIEW":
                        yield self.gen_dataset_subtype(
                            dataset_urn, DatasetSubTypes.VIEW
                        )
                    case _:
                        yield self.gen_dataset_subtype(
                            dataset_urn, DatasetSubTypes.TABLE
                        )

                # dataset properties
                yield self.gen_dataset_properties(
                    database, table, hive_table, catalog_table
                )
                # schema metadata
                yield self.gen_dataset_schema_metadata(database, table, catalog_table)

    def gen_watermark_tag(self) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=self.watermark_tag_urn, aspect=TagKeyClass("watermark")
        ).as_workunit()

    def gen_dataset_schema_metadata(
        self, database: str, table: str, catalog_table: JavaObject
    ) -> MetadataWorkUnit:
        # get table info
        schema = catalog_table.get_unresolved_schema()
        primary_key = schema.getPrimaryKey().orElse(None)
        primary_keys = primary_key.getColumnNames() if primary_key else []
        partition_keys = catalog_table.getPartitionKeys()

        # tag watermark
        watermark_specs = schema.getWatermarkSpecs()
        watermark_column = (
            watermark_specs[0].getColumnName() if watermark_specs else None
        )
        watermark_expression = (
            watermark_specs[0].toString() if watermark_specs else None
        )
        watermark_tag = (
            GlobalTagsClass([TagAssociationClass(tag=self.watermark_tag_urn)])
            if watermark_column
            else None
        )

        # construct fields
        schema_fields = []
        for column in schema.getColumns():
            name = column.getName()
            data_type = column.getDataType().toDataType(self.data_type_factory)
            column_type = data_type.getLogicalType()
            type_name = column_type.getTypeRoot().name()
            nullable = column_type.isNullable()
            comment = column.getComment()

            schema_field = SchemaFieldClass(
                fieldPath=name,
                type=SchemaFieldDataTypeClass(TYPE_MAP.get(type_name)),
                nativeDataType=type_name,
                nullable=nullable,
                description=comment,
                recursive=False,
                isPartOfKey=name in primary_keys,
                isPartitioningKey=name in partition_keys,
                globalTags=watermark_tag if name == watermark_column else None,
                jsonProps=json.dumps({watermark_expression})
                if name == watermark_expression
                else None,
            )

            schema_fields.append(schema_field)

        raw_schema = ""
        # get view query
        if catalog_table.getTableKind().name() == "VIEW":
            raw_schema = catalog_table.getOriginalQuery()
        # construct schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=f"${database}.${table}",
            platform=self.platform,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(raw_schema),
            fields=schema_fields,
            primaryKeys=primary_keys if primary_keys else None,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(database, table), aspect=schema_metadata
        ).as_workunit()

    def gen_dataset_properties(
        self,
        database: str,
        table: str,
        hive_table: JavaObject,
        catalog_table: JavaObject,
    ) -> MetadataWorkUnit:
        # put location to options
        options = catalog_table.get_options()
        options["location"] = hive_table.getSd().getLocation()

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(database, table),
            aspect=DatasetPropertiesClass(
                customProperties=options,
                name=table,
                qualifiedName=f"${database}.${table}",
                description=catalog_table.get_comment(),
            ),
        ).as_workunit()

    def gen_dataset_status(self, dataset_urn) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

    def gen_dataset_subtype(self, dataset_urn: str, subtype: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[subtype]),
        ).as_workunit()

    def add_dataset_to_dataplatform_instance(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        else:
            return None

    def gen_dataset_urn(self, database, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            self.platform,
            f"${database}.${name}",
            self.config.platform_instance,
            self.config.env,
        )

    def get_syncable_tables(self, database: str) -> list[str]:
        tables = self.catalog.list_tables(database)
        return [table for table in tables if self.config.table_pattern.allowed(table)]

    def gen_database_containers(
        self,
        database: str,
        description: Optional[str],
        properties: Optional[dict[str, str]],
    ) -> Iterable[MetadataWorkUnit]:
        catalog_key = self.gen_catalog_key()
        database_key = self.gen_database_key(database)
        yield from gen_containers(
            database_key,
            name=database,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            parent_container_key=catalog_key,
            description=description,
            extra_properties=properties,
        )

    def gen_database_key(self, database: str):
        return DatabaseKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            backcompat_env_as_instance=False,
            database=database,
        )

    def gen_catalog_containers(self) -> Iterable[MetadataWorkUnit]:
        key = self.gen_catalog_key()
        yield from gen_containers(
            container_key=key,
            name=self.config.name,
            sub_types=[DatasetContainerSubTypes.CATALOG],
        )

    def gen_catalog_key(self):
        return CatalogKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            backcompat_env_as_instance=False,
            catalog=self.config.name,
        )

    def get_syncable_databases(self) -> list[str]:
        config, catalog = self.config, self.catalog
        databases = catalog.list_databases()
        return [
            database
            for database in databases
            if config.database_pattern.allowed(database)
        ]

    def get_catalog(self):
        config = self.config
        return FlinkHiveCatalog(
            config.name,
            config.default_database,
            config.hive_version,
            config.hive_conf_dir,
            config.hive_conf,
        )
