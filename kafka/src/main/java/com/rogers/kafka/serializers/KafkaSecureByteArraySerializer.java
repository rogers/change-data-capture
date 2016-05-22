package com.rogers.kafka.serializers;

import com.rogers.kafka.crypto.key.*;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.io.IOException;

public class KafkaSecureByteArraySerializer  extends AbstractKafkaSecureByteArraySeDe implements Serializer<byte[]> {

    private String[] public_keys; 
    private static final String KEYS_CONFIG = "crypto.producer.keys";
	

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    	setKeyProvider(configs);
    	setEncryptor(configs);
    	//encryptor = EncryptorFactory.getEncryptor("binary", keyProvider);
        try { 
    	   public_keys = keyProvider.getAllPublicKeys();
        }catch(IOException e){
        	throw new ConfigException("Missing public keys" + e);
        }
    	
	    
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
    	if (data == null) return data; 
    	try{ 
            return encryptor.encrypt(data, public_keys);
    	}catch (Exception e){
    		throw new SerializationException("Error encrypting message", e);
        }
         
    }

    @Override
    public void close() {

    }
}

