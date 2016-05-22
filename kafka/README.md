Kafka on-disk encryption 
====================================================
Kafka stores all messages unencrypted on disk - [on disk](https://cwiki.apache.org/confluence/display/KAFKA/Security#Security-Encryption) encryption is currently out of scope.

This library provides a Kafka serializer and deserializer that use [digital envelope] (http://www.emc.com/emc-plus/rsa-labs/standards-initiatives/what-is-a-digital-envelope.htm) encryption 
to encrypt messages in the producer and decrypt them in the consumer ensuring that all the data on disk (and wire) is secure.

There is support for encrypting using multiple key pairs to allow multiple consumers with diffrent security privilages

It is still a work in progress, the basic functionality work, but some clean-up work is required. 
