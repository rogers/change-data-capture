package com.rogers.kafka.serializers;

import java.util.Map;

import com.rogers.kafka.crypto.key.*;

public class AbstractKafkaSecureByteArraySeDe {
		protected KeyProvider keyProvider;
		protected Encryptor encryptor;


		private String keyProviderName = "test";
		private String encryptorName = "test";
		  //TODO:  Map<?,?> is messy. Validate configs, or create a special  Config class

	    public void setKeyProvider(Map<?, ?> configs) {
	    	Object keyProviderConf = configs.get(KeyProvider.PROVIDER_CONFIG);
	    	//TODO: get this done properly
	    	if (keyProviderConf != null) {
	    		keyProviderName = (String)keyProviderConf;
	    	}
	    	keyProvider =  KeyProviderFactory.getKeyProvider(keyProviderName, configs);
	    }
	    public void setEncryptor(Map<?, ?> configs) {
	    	Object conf= configs.get(Encryptor.ENCRYPTOR_CONFIG);
	    	//TODO: get this done properly
	    	if (conf != null) {
	    		encryptorName = (String)conf;
	    	}
	       encryptor =  EncryptorFactory.getEncryptor(encryptorName, keyProvider);
	    }
	
}
