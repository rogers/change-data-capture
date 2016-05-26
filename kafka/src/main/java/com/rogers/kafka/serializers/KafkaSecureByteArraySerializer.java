package com.rogers.kafka.serializers;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class KafkaSecureByteArraySerializer extends AbstractKafkaSecureByteArraySerDe implements Serializer<byte[]> {

    private static final String KEYS_CONFIG = "crypto.producer.keys";
    private String[] public_keys;


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        AbstractKafkaSecureByteArraySerDeConfig config = new AbstractKafkaSecureByteArraySerDeConfig(configs);
        configure(config);
        try {
            public_keys = keyProvider.getAllPublicKeys();
        } catch (IOException e) {
            throw new ConfigException("Missing public keys", e);
        }
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
        if (data == null) return data;
        try {
            return encryptor.encrypt(data, public_keys);
        } catch (Exception e) {
            throw new SerializationException("Error encrypting message", e);
        }

    }

    @Override
    public void close() {

    }
}

