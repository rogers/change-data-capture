package com.rogers.kafka.serializers;

import com.rogers.kafka.crypto.key.*;
import org.apache.kafka.common.config.ConfigException;

public class EncryptorFactory {
	public static final String BINARY = "binary";
	public static final String AVRO = "avro";

	    public static Encryptor getEncryptor(String name, KeyProvider provider){

	    	Encryptor encryptor; 
	    	switch(name){
	    	case BINARY:
	    		encryptor = new  BinaryEncryptor(provider);
	    		break;
	    	case AVRO:
	    		encryptor =  new  AvroEncryptor(provider);
	    		break;
	    	default:
				throw new ConfigException("invalid Encryptor " +name  );
	    	}
	    	return encryptor; 
	    }

	
}
