package com.rogers.kafka.serializers;



import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;

public class KafkaSecureByteArrayDeserializer extends AbstractKafkaSecureByteArraySeDe implements Deserializer<byte[]>  { 


	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
		setKeyProvider(configs);
		setEncryptor(configs);
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
	    try {
	       return  encryptor.decrypt(data);
	    }catch (Exception e) {
	    	throw new SerializationException("Error decrypting  message for " , e);
	    }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
