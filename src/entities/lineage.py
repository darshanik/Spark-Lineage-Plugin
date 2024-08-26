import logging
import json

from entities.base import BaseEntity

logger = logging.getLogger(__name__)


class Lineage(BaseEntity):
    def __init__(self, items) -> None:
        super().__init__()
        self.pipelineService = ""
        self.fullyQualifiedName = ""
        self.lineage_payload_structure = {
            "edge": {
                "fromEntity": {
                    "description": "string",
                    "displayName": "string",
                    "fullyQualifiedName": "string",
                    "id": "string",
                    "name": "string",
                    "type": "string"
                },
                "lineageDetails": {
                    "columnsLineage": [],
                    "description": "string",
                    "pipeline": {
                        "deleted": False,
                        "description": "string",
                        "displayName": "string",
                        "fullyQualifiedName": "string",
                        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
                        "inherited": False,
                        "name": "string",
                        "type": "string"
                    },
                    "source": "SparkLineage",
                    "sqlQuery": "string"
                },
                "toEntity": {
                    "deleted": False,
                    "description": "string",
                    "displayName": "string",
                    "fullyQualifiedName": "string",
                    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
                    "inherited": False,
                    "name": "string",
                    "type": "string"
                }
            }
        }
    def build_lineage_edge(self, **kwargs):
        try:
            self.inputEntityId = kwargs["inputEntityId"]
            self.lineage_payload_structure["edge"]["fromEntity"]["id"] = self.inputEntityId
        except Exception as e:
            logger.error("received input entity id is invalid!!")
        try:
            self.inputEntityType = kwargs["inputEntityType"]
            self.lineage_payload_structure["edge"]["fromEntity"]["type"] = self.inputEntityType
        except Exception as e:
            logger.error("received input entity type is invalid!!")        
        try:
            self.inputEntityFullyQualifiedName  = kwargs["inputEntityFullyQualifiedName"]
            self.lineage_payload_structure["edge"]["fromEntity"]["fullyQualifiedName"] = self.inputEntityFullyQualifiedName
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["fullyQualifiedName"]
        try:
            self.inputEntityName  = kwargs["inputEntityName"]
            self.lineage_payload_structure["edge"]["fromEntity"]["name"] = self.inputEntityName
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["name"]
        try:
            self.inputEntityDisplayName  = kwargs["inputEntityDisplayName"]
            self.lineage_payload_structure["edge"]["fromEntity"]["displayName"] = self.inputEntityDisplayName
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["displayName"]
        try:
            self.inputEntityDescription  = kwargs["inputEntityDescription"]
            self.lineage_payload_structure["edge"]["fromEntity"]["description"] = self.inputEntityDescription
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["description"]
        try:
            self.inputEntityDeleted  = bool(kwargs["inputEntityDeleted"])
            self.lineage_payload_structure["edge"]["fromEntity"]["deleted"] = self.inputEntityDeleted
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["deleted"]
        try:
            self.inputEntityInherited  = bool(kwargs["inputEntityInherited"])
            self.lineage_payload_structure["edge"]["fromEntity"]["inherited"] = self.inputEntityInherited
        except:
            del self.lineage_payload_structure["edge"]["fromEntity"]["inherited"]
        try:
            self.lineageDescription  = kwargs["lineageDescription"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["description"]
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["description"]
        try:
            self.fromColumns = kwargs["fromColumns"]
            if not isinstance(self.fromColumns,list):
                logger.error(f"received columns are of type {type(self.fromColumns)}. Expecting a list!!")
            else:
                sourceColumns = self.fromColumns
        except:
            self.fromColumns = None
        try:
            self.toColumns = kwargs["toColumns"]
        except:
            self.toColumns = None
        try:
            self.function = kwargs["function"]
        except:
            self.function = None
        if self.fromColumns is not None and self.toColumns is not None:
            if self.function is not None:
                self.lineage_payload_structure["edge"]["lineageDetails"]["columnsLineage"].append(
                    {
                    "fromColumns": sourceColumns,
                    "function": self.function,
                    "toColumn": self.toColumns,
                    }
                )
            else:
                self.lineage_payload_structure["edge"]["lineageDetails"]["columnsLineage"].append(
                    {
                    "fromColumns": sourceColumns,
                    "toColumn": self.toColumns,
                    }
                )                
        else:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["columnsLineage"]
        try:
            self.lineageSource  = kwargs["lineageSource"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["source"] = self.lineageSource 
        except:
            self.lineageSource = "SparkLineage"
            del self.lineage_payload_structure["edge"]["lineageDetails"]["source"]
        try:
            self.lineageSqlQuery  = kwargs["lineageSqlQuery"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["sqlQuery"] = self.lineageSqlQuery
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["sqlQuery"]
        try:
            self.pipelineEntityId = kwargs["inputEntityId"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["id"] = self.pipelineEntityId
        except Exception as e:
            logger.error("received pipeline entity id is invalid!!")
        try:
            self.pipelineEntityType = kwargs["pipelineEntityType"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["type"] = self.pipelineEntityType
        except Exception as e:
            logger.error("received pipeline entity type is invalid!!")
        try:
            self.pipelineEntityFullyQualifiedName  = kwargs["pipelineEntityFullyQualifiedName"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["fullyQualifiedName"] = self.pipelineEntityFullyQualifiedName
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["fullyQualifiedName"]
        try:
            self.pipelineEntityName  = kwargs["pipelineEntityName"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["name"] = self.pipelineEntityName
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["name"]
        try:
            self.pipelineEntityDisplayName  = kwargs["pipelineEntityDisplayName"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["displayName"] = self.pipelineEntityDisplayName
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["displayName"]
        try:
            self.pipelineEntityDescription  = kwargs["pipelineEntityDescription"]
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["description"] = self.pipelineEntityDescription
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["description"]
        try:
            self.pipelineEntityDeleted  = bool(kwargs["pipelineEntityDeleted"])
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["deleted"] = self.pipelineEntityDeleted
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["deleted"]
        try:
            self.pipelineEntityInherited  = bool(kwargs["pipelineEntityInherited"])
            self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["inherited"] = self.pipelineEntityInherited
        except:
            del self.lineage_payload_structure["edge"]["lineageDetails"]["pipeline"]["inherited"]        
        try:
            self.outputEntityId = kwargs["outputEntityId"]
            self.lineage_payload_structure["edge"]["toEntity"]["id"] = self.outputEntityId
        except:
            logger.error("received output entity id is invalid!!")
        try:
            self.outputEntityType = kwargs["outputEntityType"]
            self.lineage_payload_structure["edge"]["toEntity"]["type"] = self.outputEntityType
        except:
            logger.error("received output entity type is invalid!!")        
        try:
            self.outputEntityFullyQualifiedName  = kwargs["outputEntityFullyQualifiedName"]
            self.lineage_payload_structure["edge"]["toEntity"]["fullyQualifiedName"] = self.outputEntityFullyQualifiedName
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["fullyQualifiedName"]
        try:
            self.outputEntityName  = kwargs["outputEntityName"]
            self.lineage_payload_structure["edge"]["toEntity"]["name"] = self.outputEntityName
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["name"]
        try:
            self.outputEntityDisplayName  = kwargs["outputEntityDisplayName"]
            self.lineage_payload_structure["edge"]["toEntity"]["displayName"] = self.outputEntityDisplayName
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["displayName"]
        try:
            self.outputEntityDescription  = kwargs["outputEntityDescription"]
            self.lineage_payload_structure["edge"]["toEntity"]["description"] = self.outputEntityDescription
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["description"]
        try:
            self.outputEntityDeleted  = bool(kwargs["outputEntityDeleted"])
            self.lineage_payload_structure["edge"]["toEntity"]["deleted"] = self.outputEntityDeleted
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["deleted"]
        try:
            self.outputEntityInherited  = bool(kwargs["outputEntityInherited"])
            self.lineage_payload_structure["edge"]["toEntity"]["inherited"] = self.outputEntityInherited
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["inherited"] 
        try:
            self.edgeDescription  = kwargs["edgeDescription"]
            self.lineage_payload_structure["edge"]["toEntity"]["description"] = self.edgeDescription
        except:
            del self.lineage_payload_structure["edge"]["toEntity"]["description"]
        
        self.lineage_payload = json.dumps(self.lineage_payload_structure)  
        logger.debug("\n spark pipeline lineage payload:")
        logger.debug(self.lineage_payload)
        return self.lineage_payload