# datacatalog-connectors

This repository contains commons code used by the Data Catalog connectors. Also it summarizes links for the connectors sample code, they leverage the [Data Catalog custom entries API](https://cloud.google.com/data-catalog/docs/how-to/custom-entries) and enable easy ingestion of metadata from a number of popular data sources. Sample code for ingesting metadata from Hive, Teradata, Oracle, SQL Server, Redshift, Vertica, Greenplum, PostgreSQL, Looker, Tableau, and others.

**Disclaimer: This is not an officially supported Google product.**

![Python package](https://github.com/GoogleCloudPlatform/datacatalog-connectors/workflows/Python%20package/badge.svg?branch=master)

## **Breaking Changes in v0.5.0**

The package names were renamed, if you are still using the older version use the branch: [release-v0.0.0](https://github.com/GoogleCloudPlatform/datacatalog-connectors/tree/release-v0.0.0)

## Project structure

Each subfolder contains a Python package. Please check components' README files for
details.

The following components are available in this repo:

| Component | Description | Folder | Language | 
|-----------|-------------|--------|----------|
| google-datacatalog-connectors-commons | Commons code shared by all connectors. | [google-datacatalog-connectors-commons](https://github.com/GoogleCloudPlatform/datacatalog-connectors/tree/master/google-datacatalog-connectors-commons) | Python |
| google-datacatalog-connectors-commons-test | Commons test code shared by all connectors. | [google-datacatalog-connectors-commons-test](https://github.com/GoogleCloudPlatform/datacatalog-connectors/tree/master/google-datacatalog-connectors-commons-test) | Python |

The following sample codes are available:

| Component | Description | Repository | Language | 
|-----------|-------------|--------|----------|
| google-datacatalog-mysql-connector | Sample code for MySQL data source. | [google-datacatalog-mysql-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-mysql-connector) | Python |
| google-datacatalog-postgresql-connector | Sample code for PostgreSQL data source. | [google-datacatalog-postgresql-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-postgresql-connector) | Python |
| google-datacatalog-sqlserver-connector | Sample code for SQLServer data source. | [google-datacatalog-sqlserver-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-sqlserver-connector) | Python |
| google-datacatalog-redshift-connector | Sample code for Redshift data source. | [google-datacatalog-redshift-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-redshift-connector) | Python |
| google-datacatalog-oracle-connector | Sample code for Oracle data source. | [google-datacatalog-oracle-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-oracle-connector) | Python |
| google-datacatalog-teradata-connector | Sample code for Teradata data source. | [google-datacatalog-teradata-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-teradata-connector) | Python |
| google-datacatalog-vertica-connector | Sample code for Vertica data source. | [google-datacatalog-vertica-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-vertica-connector) | Python |
| google-datacatalog-greenplum-connector | Sample code for Greenplum data source. | [google-datacatalog-greenplum-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-greenplum-connector) | Python |
| google-datacatalog-rdbmscsv-connector | Sample code for generic RDBMS CSV ingestion. | [google-datacatalog-rdbmscsv-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-rdbmscsv-connector) | Python |
| google-datacatalog-saphana-connector | Sample code for Sap Hana data source. | [google-datacatalog-saphana-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-rdbms/tree/master/google-datacatalog-saphana-connector) | Python |
| google-datacatalog-looker-connector |  Sample code for Looker data source. | [google-datacatalog-looker-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/tree/master/google-datacatalog-looker-connector) | Python |
| google-datacatalog-qlik-connector | Sample code for Qlik Sense data source. | [google-datacatalog-qlik-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/tree/master/google-datacatalog-qlik-connector) | Python |
| google-datacatalog-tableau-connector | Sample code for Tableau data source. | [google-datacatalog-tableau-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/tree/master/google-datacatalog-tableau-connector) | Python | 
| google-datacatalog-hive-connector | Sample code for Hive data source. | [google-datacatalog-hive-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/google-datacatalog-hive-connector) | Python |
| google-datacatalog-apache-atlas-connector | Sample code for Apache Atlas data source. | [google-datacatalog-apache-atlas-connector](https://github.com/GoogleCloudPlatform/datacatalog-connectors-hive/tree/master/google-datacatalog-apache-atlas-connector) | Python |

