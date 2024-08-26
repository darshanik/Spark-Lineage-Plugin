import logging

from entities.base import BaseEntity

logger = logging.getLogger(__name__)


class Table(BaseEntity):
    def __init__(self, items) -> None:
        super().__init__()
        if "symlinks" in items["facets"]:
            for identifier in items["facets"]["symlinks"]["identifiers"]:
                self.tableName = identifier["name"]
                self.databaseName = identifier["namespace"]
        else:
            self.tableName = None
            self.databaseName = None
        if "columnLineage" in items["facets"]:
            if "fields" in items["facets"]["columnLineage"]:
                self.columnlineage = items["facets"]["columnLineage"]["fields"]
        else:
            self.columnlineage = []
        if "schema" in items["facets"]:
            if "fields" in items["facets"]["schema"]:
                self.fields = items["facets"]["schema"]["fields"]
            else:
                self.fields = []
        self.serviceName = "Generic_Service_Name"
        self.tableFullyQualifiedName = self.serviceName + self.databaseName + self.tableName
    
    def get_tableName(self):
        return self.tableName

    def get_qualifiedName(self):
        return self.fullyQualifiedName

    def get_databaseName(self):
        return self.databaseName

    def get_columns(self):
        return self.fields

    def get_typeName(self):
        return self.openmetadata_table_typename

    def get_columnLineage(self):
        return self.columnlineage

    def get_entity_id(self):
        tableParameters = self.fullyQualifiedName
        try:
            response = self.get_from_openmetadata_api(tableParameters)
            entityId = response["id"]
            return entityId, response
        except Exception as e:
            logger.error("Error getting entity",e)
            return None

    def get_from_and_to_columns(self):
        fromColumns = []
        toColumns = []
        if isinstance(self.columnlineage, dict):
            for dest_column in self.columnlineage.keys():
                for source_column in self.columnlineage[dest_column]["inputFields"]:
                    sourceTableName = source_column["name"]
                    sourceDatabaseName = source_column["namespace"]
                    sourceField = source_column["field"]
                    sourceColumnFullyQualifiedName = self.serviceName + sourceDatabaseName + sourceTableName + sourceField
                    fromColumns.append(sourceColumnFullyQualifiedName)
                destColumnFullyQualifiedName = self.tableFullyQualifiedName + self.dot + dest_column
                toColumns.append(destColumnFullyQualifiedName)
                # TODO: parse transformation description and type from openlineage
            return fromColumns, toColumns
        else:
            logger.warn(f"Received type for columnLineage {type(self.columnlineage)}. Expecting dictionary.")


            
                
            
        