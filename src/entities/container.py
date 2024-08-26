import logging

from entities.base import BaseEntity

logger = logging.getLogger(__name__)

class Container(BaseEntity):
    def __init__(self, items) -> None:
        self.containerName = items["name"].replace("/",".")
        self.fullyQualifiedName = self.openmetadata_container_service + self.containerName
    def get_containerName(self):
        return self.containerName
    def get_containerFullyQualifiedName(self):
        return self.fullyQualifiedName
    def get_entity_id(self):
        container_unique_endpoint = self.openmetadata_container_parameters + self.slash + self.fullyQualifiedName
        try:
            response = self.get_from_openmetadata_api(container_unique_endpoint)
            container_id = response["id"]
            return container_id
        except Exception as e:
            logger.error("Error getting container id from open metadata!!. Check if the entity is existing!!")
            return None
      