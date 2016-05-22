package com.rogers.kafka.serializers;


import com.rogers.kafka.crypto.key.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;
import java.io.IOException;
import java.util.Properties;

public class KafkaSecureByteArrayDeserializer extends AbstractKafkaSecureByteArraySeDe implements Deserializer<byte[]>  { 


	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
		setKeyProvider(configs);
		setEncryptor(configs);
    	//encryptor = EncryptorFactory.getEncryptor("binary", keyProvider);
    }

    @Override
    public byte[] deserialize(String topic, byte[] data) {
    	System.out.println("KafkaDeserializer, input message =  "   + Hex.encodeHexString(data));
	    //EncryptedMessage msg   = new EncryptedMessage(data);
	    try { 
	    	System.out.println("KafkaDeserializer, output message =  "   +encryptor.decryptToString(data));
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
