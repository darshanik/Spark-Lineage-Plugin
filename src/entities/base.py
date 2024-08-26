import logging
import json
import requests
from entities.utils import get_env_var

logger = logging.getLogger(__name__)


class BaseEntity:

    def __init__(self) -> None:
        self.typeName = None
        self.qualifiedName = None

        '''Initialize the environment variables '''

        self.__openmetadata_token = get_env_var("OPENMETADATA_TOKEN")
        self.openmetadata_host = get_env_var('OPENMETADATA_HOST')
        self.openmetadata_conn = f"http://{self.openmetadata_host}/api/v1"
        self.openmetadata_api_hdr = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.__openmetadata_token}'
        }

        self.sep = get_env_var("SPARK_LINEAGE_PLUGIN_SEPERATOR")
        self.slash = get_env_var("SPARK_LINEAGE_PLUGIN_SLASH_SEPERATOR")
        self.dot = get_env_var("SPARK_LINEAGE_PLUGIN_DOT_SEPERATOR")
        self.underscore = get_env_var(
            "SPARK_LINEAGE_PLUGIN_UNDERSCORE_SEPERATOR"
        )

        self.cluster = get_env_var("SPARK_LINEAGE_PLUGIN_CLUSTER")
        self.openmetadata_spark_lineage_parameters = get_env_var(
            "OPENMETADATA_SPARK_LINEAGE_PARAMETERS")
        self.openmetadata_spark_pipeline_parameters = get_env_var(
            "OPENMETADATA_SPARK_PIPELINE_PARAMETERS")
        self.openmetadata_table_parameters = get_env_var(
            "OPENMETADATA_TABLE_PARAMETERS")
        self.openmetadata_container_parameters = get_env_var(
            "OPENMETADATA_CONTAINER_PARAMETERS")
        self.openmetadata_table_typename = get_env_var(
            "OPENMETADATA_TABLE_TYPENAME")
        self.openmetadata_container_typename = get_env_var(
            "OPENMETADATA_CONTAINER_TYPENAME")

        self.openmetadata_container_service = get_env_var(
            "OPENMETADATA_CONTAINER_SERVICE")

        self.openmetadata_spark_pipeline_service = get_env_var(
            "OPENMETADATA_SPARK_PIPELINE_SERVICE")

        self.openmetadata_hive_service = get_env_var(
            "OPENMETADATA_HIVE_SERVICE")
        self.openmetadata_trino_service = get_env_var(
            "OPENMETADATA_TRINO_SERVICE")
        self.openmetadata_presto_service = get_env_var(
            "OPENMETADATA_PRESTO_SERVICE")
        self.openmetadata_mysql_service = get_env_var(
            "OPENMETADATA_MYSQL_SERVICE")
        self.openmetadata_oracle_service = get_env_var(
            "OPENMETADATA_ORACLE_SERVICE")
        self.openmetadata_postgresql_service = get_env_var(
            "OPENMETADATA_POSTGRESQL_SERVICE")

        self.openmetadata_snowflake_service = get_env_var(
            "OPENMETADATA_SNOWFLAKE_SERVICE")
        self.openmetadata_athena_service = get_env_var(
            "OPENMETADATA_ATHENA_SERVICE")
        self.openmetadata_azuresql_service = get_env_var(
            "OPENMETADATA_AZURESQL_SERVICE")
        self.openmetadata_bigquery_service = get_env_var(
            "OPENMETADATA_BIGQUERY_SERVICE")
        self.openmetadata_databricks_service = get_env_var(
            "OPENMETADATA_DATABRICKS_SERVICE")
        self.openmetadata_db2_service = get_env_var("OPENMETADATA_DB2_SERVICE")
        self.openmetadata_datalake_service = get_env_var(
            "OPENMETADATA_DATALAKE_SERVICE")
        self.openmetadata_mongodb_service = get_env_var(
            "OPENMETADATA_MONGODB_SERVICE")
        self.openmetadata_mariadb_service = get_env_var(
            "OPENMETADATA_MARIADB_SERVICE")

    def push_to_openmetadata_api(self, parameters, api_call="POST", payload=None):
        response = requests.request(
            api_call,
            self.openmetadata_conn + self.slash + parameters,  # type: ignore
            headers=self.openmetadata_api_hdr,
            data=payload
        )
        logger.debug(response.text)

    def get_from_openmetadata_api(self, parameters):
        response = requests.request(
            "GET",
            self.openmetadata_conn + self.slash + parameters,  # type: ignore
            headers=self.openmetadata_api_hdr
        )
        response_dict = json.loads(response.text)
        return response_dict
