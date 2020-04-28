# datacatalog-connectors

This repository contains commons code used by the Data Catalog connectors. Also it summarizes links for the connectors sample code, they leverage the [Data Catalog custom entries API](https://cloud.google.com/data-catalog/docs/how-to/custom-entries) and enable easy ingestion of metadata from a number of popular data sources. Sample code for ingesting metadata from Hive, Teradata, Oracle, SQL Server, Redshift, Vertica, Greenplum, PostgreSQL, Looker, Tableau, and others.

**Disclaimer: This is not an officially supported Google product.**

## Project structure

Each subfolder contains a Python package. Please check components' README files for
details.

The following components are available in this repo:

| Component | Description | Folder | Language | 
|-----------|-------------|--------|----------|
| datacatalog-connectors-commons | Commons code shared by all connectors. | [./datacatalog-connectors-commons](https://github.com/GoogleCloudPlatform/datacatalog-connectors/tree/master/datacatalog-connectors-commons) | Python |
| datacatalog-connectors-test-commons | Commons test code shared by all connectors. | [./datacatalog-connectors-test-commons](https://github.com/GoogleCloudPlatform/datacatalog-connectors/tree/master/datacatalog-connectors-test-commons) | Python |

The following sample codes are available:

| Component | Description | Repository | Language | 
|-----------|-------------|--------|----------|
| mysql2datacatalog | Sample code for MySQL data source. | [mysql2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/mysql2datacatalog) | Python |
| postgresql2datacatalog | Sample code for PostgreSQL data source. | [postgresql2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/postgresql2datacatalog) | Python |
| sqlserver2datacatalog | Sample code for SQLServer data source. | [sqlserver2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/sqlserver2datacatalog) | Python |
| redshift2datacatalog | Sample code for Redshift data source. | [redshift2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/redshift2datacatalog) | Python |
| oracle2datacatalog | Sample code for Oracle data source. | [oracle2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/oracle2datacatalog) | Python |
| teradata2datacatalog | Sample code for Teradata data source. | [teradata2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/teradata2datacatalog) | Python |
| vertica2datacatalog | Sample code for Vertica data source. | [vertica2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/vertica2datacatalog) | Python |
| greenplum2datacatalog | Sample code for Greenplum data source. | [greenplum2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/greenplum2datacatalog) | Python |
| rdbmscsv2datacatalog | Sample code for generic RDBMS CSV ingestion. | [rdbmscsv2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/rdbmscsv2datacatalog) | Python |
| looker2datacatalog |  Sample code for Looker data source. | [looker2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/tree/master/looker2datacatalog) | Python |
| tableau2datacatalog | Sample code for Tableau data source. | [tableau2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/tree/master/tableau2datacatalog) | Python | 
| hive2datacatalog | Sample code for Hive data source. | [hive2datacatalog](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/hive2datacatalog) | Python |
