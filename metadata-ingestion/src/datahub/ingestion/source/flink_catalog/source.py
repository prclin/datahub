import json
from typing import Iterable, Optional

from pydantic import Field, ValidationError, model_validator

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
from datahub.ingestion.source.flink_catalog.catalog import (
    CatalogTable,
    FlinkHiveCatalog,
    HiveTable,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata._internal_schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    TagKeyClass,
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


class FlinkCatalogSource(StatefulIngestionSourceBase):
    platform: str = "flink"
    watermark_tag_urn: str = make_tag_urn("flink.watermark")

    def __init__(self, config: FlinkCatalogConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.ctx = ctx
        self.report = FlinkCatalogReport()
        self.catalog = self.get_catalog()
        self.catalog.open()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = FlinkCatalogConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> FlinkCatalogReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # add flink watermark tag
        yield self.gen_watermark_tag()

        # catalog container
        yield from self.gen_catalog_containers()

        # database level
        databases = self.get_syncable_databases()
        for database in databases:
            db = self.catalog.get_database(database)
            description = db.get_comment()
            properties = db.get_properties()
            yield from self.gen_database_containers(database, description, properties)
            # table and view level
            tables = self.get_syncable_tables(database)
            for table in tables:
                dataset_urn = self.gen_dataset_urn(table)
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
                hive_table = self.catalog.get_hive_table(database, table)
                catalog_table = self.catalog.instantiate_catalog_table(hive_table)

                # subtype
                table_kind = catalog_table.get_table_kind()
                yield self.gen_dataset_subtype(
                    dataset_urn,
                    DatasetSubTypes.VIEW
                    if table_kind == "VIEW"
                    else DatasetSubTypes.TABLE,
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
        self, database: str, table: str, catalog_table: CatalogTable
    ) -> MetadataWorkUnit:
        # get table info
        schema = catalog_table.get_unresolved_schema()
        primary_keys = schema.get_primary_keys()
        partition_keys = catalog_table.get_partition_keys()

        # tag watermark
        watermark_column, watermark_expression = schema.get_watermark_spec()
        watermark_tag = (
            GlobalTagsClass([TagAssociationClass(tag=self.watermark_tag_urn)])
            if watermark_column
            else None
        )

        # construct fields
        schema_fields = []
        for column in schema.get_columns():
            name = column.name
            column_type = column.column_type

            schema_field = SchemaFieldClass(
                fieldPath=name,
                type=SchemaFieldDataTypeClass(column_type.type_class),
                nativeDataType=column_type.native_data_type,
                nullable=column_type.nullable,
                description=column.comment,
                recursive=False,
                isPartOfKey=name in primary_keys,
                isPartitioningKey=name in partition_keys,
                globalTags=watermark_tag if name == watermark_column else None,
                jsonProps=json.dumps(watermark_expression)
                if name == watermark_column
                else None,
            )
            schema_fields.append(schema_field)
            # if column's type is Row, add subcolumns
            if column_type.fields:
                for field, description, field_type in column_type.fields:
                    field_name = f"{name}.{field}"
                    schema_field = SchemaFieldClass(
                        fieldPath=field_name,
                        type=SchemaFieldDataTypeClass(field_type.type_class),
                        nativeDataType=field_type.native_data_type,
                        nullable=field_type.nullable,
                        description=description,
                        recursive=False,
                        isPartOfKey=field_name in primary_keys,
                        isPartitioningKey=field_name in partition_keys,
                        globalTags=watermark_tag
                        if field_name == watermark_column
                        else None,
                        jsonProps=json.dumps({watermark_expression})
                        if field_name == watermark_column
                        else None,
                    )
                    schema_fields.append(schema_field)

        raw_schema = ""
        # get view query
        if catalog_table.get_table_kind() == "VIEW":
            raw_schema = catalog_table.get_original_query()
        # construct schema metadata
        schema_metadata = SchemaMetadataClass(
            schemaName=f"{database}.{table}",
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(raw_schema),
            fields=schema_fields,
            primaryKeys=primary_keys if primary_keys else None,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(table), aspect=schema_metadata
        ).as_workunit()

    def gen_dataset_properties(
        self,
        database: str,
        table: str,
        hive_table: HiveTable,
        catalog_table: CatalogTable,
    ) -> MetadataWorkUnit:
        # put location to options
        options = catalog_table.get_options()
        options["location"] = hive_table.get_location()

        return MetadataChangeProposalWrapper(
            entityUrn=self.gen_dataset_urn(table),
            aspect=DatasetPropertiesClass(
                customProperties=options,
                name=table,
                qualifiedName=f"{database}.{table}",
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

    def gen_dataset_urn(self, table: str) -> str:
        return make_dataset_urn_with_platform_instance(
            make_data_platform_urn(self.platform),
            table,
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
            container_key=database_key,
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

    def get_catalog(self) -> FlinkHiveCatalog:
        config = self.config
        return FlinkHiveCatalog(
            config.name,
            config.default_database,
            config.hive_version,
            config.hive_conf_dir,
            config.hive_conf,
        )
