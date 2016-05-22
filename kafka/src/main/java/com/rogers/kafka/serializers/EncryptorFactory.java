package com.rogers.kafka.serializers;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.rogers.kafka.crypto.key.*;

public class EncryptorFactory {

	    public static Encryptor getEncryptor(String name, KeyProvider provider){

	    	Encryptor encryptor; 
	    	switch(name){
	    	case "binary":
	    		encryptor = new  BinaryEncryptor(provider);
	    		break;
	    	case "avro":
	    		encryptor =  new  AvroEncryptor(provider);
	    		break;
	    	default:
	    		encryptor = new  BinaryEncryptor(provider);
	    		break;
	    	}
	    	return encryptor; 
	    }

	
}
