package com.rogers.kafka.serializers;

import java.util.Map;

import com.rogers.kafka.crypto.key.*;

public class AbstractKafkaSecureByteArraySerDe {
		protected KeyProvider keyProvider;
		protected Encryptor encryptor;

	    public void configure(AbstractKafkaSecureByteArraySerDeConfig config) {

			String keyProviderName = config.getString(AbstractKafkaSecureByteArraySerDeConfig.KEY_PROVIDER_CONFIG);
			keyProvider =  KeyProviderFactory.getKeyProvider(keyProviderName, config.originals);

			String encryptorName = config.getString(AbstractKafkaSecureByteArraySerDeConfig.ENCRYPTOR_CONFIG);
			encryptor =  EncryptorFactory.getEncryptor(encryptorName, keyProvider);
		}
}
