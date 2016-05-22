package com.rogers.kafka.serializers;

import org.apache.kafka.common.errors.SerializationException;


import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KafkaSecureByteArrayDecoder extends AbstractKafkaSecureByteArraySeDe  implements Decoder<byte[]> {


  /**
   * Constructor used by Kafka consumer.
   */
  public KafkaSecureByteArrayDecoder(VerifiableProperties props) {
    setKeyProvider(props.props());
    setEncryptor(props.props());
	//encryptor =  EncryptorFactory.getEncryptor("binary", keyProvider);
  }

  @Override
  public byte[] fromBytes(byte[] bytes) {
	  //TODO: Put this in a new Abstract class for both Decoder and Deserializer
	 // EncryptedMessage msg   = new EncryptedMessage(bytes);
	    try { 
	       return  encryptor.decrypt(bytes);
	    }catch (Exception e) {
	    	throw new SerializationException("Error decrypting  message for " , e);
	    }
  }
}
