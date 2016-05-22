A change data capture framework for  Confluent Kafka
====================================================

A framework for creating change data capture systems for Confluent Kafka


## Compiling
This library uses some features that have been added to Confluent schema-registry 2.1.0. Confluent has not  published this version (they skipped to 3.0.0), so you will have to build it yourself. Bellow are the sources we used  
* schema-registry - https://github.com/confluentinc/schema-registry/commit/1918d7e3277a53df91162abc0089aca131e5c23c
* rest-util - https://github.com/confluentinc/rest-utils/tree/161ae74484f21f6e8dfba1f2cabc77dcbd8bd2e6
* common - https://github.com/confluentinc/common/tree/e9fc841dde53293b3fa5b223346d277233ad3e88


## Features (Need to add details)
* Integration with Confluent Kafka
* An generic framework to integrate with Change Data Captures systems such as Oracle GoldenGate 
