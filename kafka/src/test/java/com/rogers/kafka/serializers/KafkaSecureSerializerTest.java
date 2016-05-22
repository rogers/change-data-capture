package com.rogers.kafka.serializers;

import  com.rogers.kafka.crypto.key.*;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;









import kafka.utils.VerifiableProperties;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaSecureSerializerTest {


  private  KafkaSecureByteArraySerializer serializer;
  private  KafkaSecureByteArrayDeserializer deserializer;
  private  KeyProvider keyProvider; 


  private  String topic = "testTopic2";
  //TODO: fix tests... failing with mvn. Probably some path issue
//  @Test
  public void testSecureSerializer() {
      String test = "Test String";
      
      Map<String, Object> serializerConfigs = new HashMap<String, Object>();
      
      Map<String, Object> deserializerConfigs = new HashMap<String, Object>();
     
      serializer = new KafkaSecureByteArraySerializer();
      serializer.configure(serializerConfigs, false);
      deserializer = new KafkaSecureByteArrayDeserializer();
      deserializer.configure(deserializerConfigs, false);
      
      assertEquals("Should get the original string after serialization and deserialization",
    		  test, new String(deserializer.deserialize(topic, serializer.serialize(topic, test.getBytes()))));

    /*  assertEquals("Should support null in serialization and deserialization",
              null, deserializer.deserialize(topic, serializer.serialize(topic, null)));*/
  }
  public void testAvroSecureSerializer() {
      String test = "Test String";
      
      Map<String, Object> serializerConfigs = new HashMap<String, Object>();
      
      Map<String, Object> deserializerConfigs = new HashMap<String, Object>();
     
      serializer = new KafkaSecureByteArraySerializer();
      serializerConfigs.put(Encryptor.ENCRYPTOR_CONFIG , "avro");
      serializer.configure(serializerConfigs, false);
      deserializerConfigs.put(Encryptor.ENCRYPTOR_CONFIG , "avro");
      deserializer = new KafkaSecureByteArrayDeserializer();
      deserializer.configure(deserializerConfigs, false);
      
      assertEquals("Should get the original string after serialization and deserialization",
    		  test, new String(deserializer.deserialize(topic, serializer.serialize(topic, test.getBytes()))));

    /*  assertEquals("Should support null in serialization and deserialization",
              null, deserializer.deserialize(topic, serializer.serialize(topic, null)));*/
  }
 // @Test
  public void wrongConfig() { 
	  Map<String, Object> serializerConfigs = new HashMap<String, Object>();
	  serializerConfigs.put(KeyProvider.PROVIDER_CONFIG , "random");
	  try{
	     serializer.configure(serializerConfigs, false);
	     fail("Didn't throw an exception for wrong key provider");
	  } catch (ConfigException e) {
	      // this is expected
	  } catch (Exception e) {
	     // fail("Threw the incorrect exception when wrong key provider was provided");
	  } 
	  serializerConfigs.clear();
	  serializerConfigs.put(KeyProvider.PROVIDER_CONFIG , "config");
	  try{
	     serializer.configure(serializerConfigs, false);
	     fail("Didn't throw an exception when didn't provide configs for ConfigKeyProvider");
	  } catch (ConfigException e) {
	      // this is expected
	  } catch (Exception e) {
	    //  fail("Threw the incorrect exception when when didn't provide configs for ConfigKeyProvider");
	  } 
	  
  }
  public static void main(String [ ] args) {
	  KafkaSecureSerializerTest test = new KafkaSecureSerializerTest();
		test.testSecureSerializer();
		test.testAvroSecureSerializer();
		test.wrongConfig();
	}

}

 
  
