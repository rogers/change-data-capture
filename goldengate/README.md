Oracle Golden Gate Adapter for Confluent Kafka
==============================================

## Compiling 
The package depends on ggjave. 
Download GoldenGate from the Oracle website and modify the pom file to point to ggjave.
Or just install the GoldenGate  jars in your repository. 


## Installing and Using
Please refer to the GoldenGate Adapter documentation for a detailed guide (docs are avalible as part of the GoldenGate download)

## Components

### CleanAbastractHandler
GoldenGate comes with several adapter implementations (HDFS, Flume, JMX) - each has it's own source code, and is implemented diffrently (some are very buggy) 
This is an attempt to create a common base class that can be reusedto to build adapters, and provide all the benefits of modular design.

### mutationmappers
A set of classes to intigrate GoldenGate with the change-data-capture library

### KafkaHandler
An implementation of a GoldenGate handler for Kafka

