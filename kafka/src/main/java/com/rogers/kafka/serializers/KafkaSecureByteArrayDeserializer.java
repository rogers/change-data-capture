package com.rogers.kafka.serializers;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaSecureByteArrayDeserializer extends AbstractKafkaSecureByteArraySerDe implements Deserializer<byte[]> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        AbstractKafkaSecureByteArraySerDeConfig config = new AbstractKafkaSecureByteArraySerDeConfig(configs);
        configure(config);
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
        try {
            return encryptor.decrypt(data);
        } catch (Exception e) {
            throw new SerializationException("Error decrypting  message for ", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
