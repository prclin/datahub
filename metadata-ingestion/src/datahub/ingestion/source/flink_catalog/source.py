import json
from typing import Iterable, Literal, Optional, Union

from pydantic import Field, ValidationError, model_validator
from pyflink.table.catalog import Catalog, JdbcCatalog, ObjectPath

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

kinds = ["hive", "jdbc"]
FlinkCatalogType = Literal["hive", "jdbc"]


class FlinkCatalogConfig(StatefulIngestionConfigBase):
    default_database: str = "default"

    class BaseConf:
        name: str = Field(
            default=None,
            description="catalog name,default as catalog type hive or jdbc",
        )
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

    class JdbcCatalogConf(BaseConf):
        username: str = Field(description="jdbc username.")
        password: str = Field(description="jdbc password.")
        url: str = Field(description="jdbc url")

    class HiveCatalogConf(BaseConf):
        hive_version: str = Field(
            default=None, description="hive version string like 3.1.3"
        )
        hive_conf_dir: str = Field(
            default=None,
            description="hive configuration directory,which contains the hive-site.xml.",
        )
        hive_conf: dict[str, str] = Field(
            default=None, description="hive configurations."
        )

    hive_catalog: HiveCatalogConf = Field(
        default=None,
        description="flink hive catalog configuration, see also at https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/hive/overview/#connecting-to-hive",
    )
    jdbc_catalog: JdbcCatalogConf = Field(
        default=None,
        description="flink jdbc catalog configuration, see also at https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/#usage-of-jdbc-catalog",
    )
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
        one of the hive_catalog and jdbc_catalog should be not none.
        in hive_catalog, one of hive_conf and hive_conf_dir should be not none.
        if catalog name is not set,complete it.
        """
        if not self.hive_catalog and not self.jdbc_catalog:
            raise ValidationError("should specify at least one catalog config!")
        if self.hive_catalog:
            if not self.hive_catalog.hive_conf and self.hive_catalog.hive_conf_dir:
                raise ValidationError("should specify at least one hive config type!")
        if self.hive_catalog and not self.hive_catalog.name:
            self.hive_catalog.name = "hive"
        if self.jdbc_catalog and not self.jdbc_catalog.name:
            self.jdbc_catalog.name = "jdbc"
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
    "VARCHAR": BooleanTypeClass,
    "NullClass": NullTypeClass,
}


class FlinkCatalogSource(metaclass=StatefulIngestionSourceBase):
    platform: str = "flink"
    watermark_tag_urn: str = make_tag_urn("flink.watermark")

    def __init__(self, config: FlinkCatalogConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.ctx = ctx
        self.report = FlinkCatalogReport()
        self.hive_catalog = self.get_hive_catalog()
        self.jdbc_catalog = self.get_jdbc_catalog()

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        # add flink watermark tag
        yield self.gen_watermark_tag()

        kind: FlinkCatalogType
        for kind in kinds:
            config, catalog = self.get_config_and_catalog(kind)
            if not config:
                continue

            # catalog level
            yield from self.gen_catalog_containers(kind)
            # database level
            databases = self.get_syncable_databases(kind)
            for database in databases:
                db = catalog.get_database(database)
                description = db.get_comment()
                properties = db.get_properties()
                yield from self.gen_database_containers(
                    kind, database, description, properties
                )
                # todo 获取表和视图信息
                # table level
                tables = self.get_syncable_tables(kind, database)
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
                    # subtype
                    yield self.gen_table_subtype(dataset_urn)
                    # status
                    yield self.gen_table_status(dataset_urn)
                    # dataset properties
                    yield self.gen_table_properties(kind, database, table)
                    # schema metadata
                    yield self.gen_table_schema_metadata(kind, database, table)
                    # todo 获取位置血缘

    def gen_watermark_tag(self) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=self.watermark_tag_urn, aspect=TagKeyClass("watermark")
        ).as_workunit()

    def gen_table_schema_metadata(
        self, kind: FlinkCatalogType, database: str, table: str
    ) -> MetadataWorkUnit:
        _, catalog = self.get_config_and_catalog(kind)

        # get table info
        tb = catalog.get_table(ObjectPath(database, table))
        schema = tb.get_unresolved_schema()._j_schema
        primary_key = schema.getPrimaryKey().orElse(None)
        primary_keys = primary_key.getColumnNames() if primary_key else []
        partition_keys = tb._j_catalog_base_table.getPartitionKeys()

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
            column_type = column.getDataType().getLogicalType()
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

        # construct schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=f"${database}.${table}",
            platform=self.platform,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(""),
            fields=schema_fields,
            primaryKeys=primary_keys if primary_keys else None,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(database, table), aspect=schema_metadata
        ).as_workunit()

    def gen_table_properties(
        self, kind: FlinkCatalogType, database: str, table: str
    ) -> MetadataWorkUnit:
        _, catalog = self.get_config_and_catalog(kind)
        tb = catalog.get_table(ObjectPath(database, table))
        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(database, table),
            aspect=DatasetPropertiesClass(
                customProperties=tb.get_options(),
                name=table,
                qualifiedName=f"${database}.${table}",
                description=tb.get_comment(),
            ),
        ).as_workunit()

    def gen_table_status(self, table_urn) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=table_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

    def gen_table_subtype(self, table_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=table_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
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

    def get_syncable_tables(self, kind: FlinkCatalogType, database: str) -> list[str]:
        config, catalog = self.get_config_and_catalog(kind)
        tables = catalog.list_tables(database)
        return [table for table in tables if config.table_pattern.allowed(table)]

    def gen_database_containers(
        self,
        kind: FlinkCatalogType,
        database: str,
        description: Optional[str],
        properties: Optional[dict[str, str]],
    ) -> Iterable[MetadataWorkUnit]:
        catalog_key = self.gen_catalog_key(kind)
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

    def gen_catalog_containers(
        self, kind: FlinkCatalogType
    ) -> Iterable[MetadataWorkUnit]:
        key = self.gen_catalog_key(kind)
        config, catalog = self.get_config_and_catalog(kind)
        yield from gen_containers(
            container_key=key,
            name=config.name,
            sub_types=[DatasetContainerSubTypes.CATALOG],
        )

    def gen_catalog_key(self, kind: FlinkCatalogType):
        config, _ = self.get_config_and_catalog(kind)
        return CatalogKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            backcompat_env_as_instance=False,
            catalog=config.name,
        )

    def get_config_and_catalog(
        self, kind: FlinkCatalogType
    ) -> tuple[
        Union[FlinkCatalogConfig.HiveCatalogConf, FlinkCatalogConfig.JdbcCatalogConf],
        Catalog,
    ]:
        match kind:
            case "hive":
                return self.config.hive_catalog, self.hive_catalog
            case "jdbc":
                return self.config.jdbc_catalog, self.jdbc_catalog
            case _:
                raise ValueError("kind should be hive or jdbc")

    def get_syncable_databases(self, kind: FlinkCatalogType) -> list[str]:
        config, catalog = self.get_config_and_catalog(kind)
        databases = catalog.list_databases()
        return [
            database
            for database in databases
            if config.database_pattern.allowed(database)
        ]

    def get_hive_catalog(self):
        config = self.config
        return (
            FlinkHiveCatalog(
                config.hive_catalog.name,
                config.default_database,
                config.hive_conf,
                config.hive_version,
            )
            if config.hive_catalog
            else None
        )

    def get_jdbc_catalog(self):
        config = self.config
        return (
            JdbcCatalog(
                config.jdbc_catalog.name,
                config.default_database,
                config.jdbc_conf.username,
                config.jdbc_conf.password,
                config.jdbc_conf.url,
            )
            if config.jdbc_catalog
            else None
        )
