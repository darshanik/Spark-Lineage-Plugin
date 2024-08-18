# Overview
This plugin Collects and Transforms the facets emitted by Open Lineage Spark Listener into OpenMetadata Compatible payload definition that creates/updates the lineage.
# Features
* Container-To-Table Lineage
* Table-to-Table Lineage
* Column-Level Lineage
# Pre-Requisites
* Spark Cluster or Spark Operator with OpenLineage Integration(i.e., your spark job or cluster must be having open-lineage jar Installed)
* OpenLineage spark listener version >= 0.20.6 enabled
* OpenLineage spark listener configured to emit event messages to a kafka topic or API of this plugin
# Limitations
* Generates Lineage in OpenMetadata for only spark jobs. 
# Note: This Document is subject to change as more features will be released 

