## Integration Details

this plugin ingest flink tables and views which stored in hive.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept                                                    | Notes |
| -------------- | ------------------------------------------------------------------ | ----- |
| flink          | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |       |
| table or view  | [Dataset](docs/generated/metamodel/entities/dataset.md)            |       |
| catalog        | Container                                                          |       |
| database       | Container                                                          |       |
| watermark      | Tag                                                                |       |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from Flink Catalog, you will need:

- java 8+

### Install the Plugin

Run the following commands to install the relevant plugin:

`pip install 'acryl-datahub[flink-catalog]'`

### Configure the Ingestion Recipe

Use the following recipe to get started with ingestion.

```yml
source:
  type: flink-catalog
  config:
    # database patterns
    database_pattern:
      allow:
        - dwd
    # table patterns
    table_pattern:
      allow:
        - tmp_table
    hive_version: 3.1.3
    # hive conf dir
    hive_conf_dir: /home/xxx/hive/conf
    # hive conf
    # hive_conf:
    #   hive.metastore.uris: thrift://xxx:9083
# sink configs
```
