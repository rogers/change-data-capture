package com.rogers.kafka.serializers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.utils.VerifiableProperties;
import kafka.serializer.DefaultDecoder;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaSecureProducerTest {
	private Properties config;
	private KafkaProducer<byte[],byte[]> producer;
	InputStream inputStream;
	private final String KAFKA_CONFIG_FILE = "kafka.properties";
	private String topic = "test_topic2";
	private ConsumerConnector consumer;
	private final int MSG_COUNT = 1000; 
	private final String TEST_STRING = "Test String";
	
	private void initConfig(String propFileName) throws IOException{
	       config = new Properties();
			try {
			  inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			  if (inputStream != null) {
				config.load(inputStream);
			  } else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			  }
			} finally {
				inputStream.close();
			}
	    }
	private void initProducer(){
		try{ 
		  initConfig(KAFKA_CONFIG_FILE);
		}  catch (Exception e) {
			fail("failed to create a read config file");
	    }   
		try {
			 producer = new KafkaProducer<byte[],byte[]>(config);
		}  catch (Exception e) {
			throw e; 
			//fail("failed to create a producer" + e);
	    } 
		
	 }
    private static Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "52.4.197.159:2181");
        props.put("group.id", randGroupName());
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","smallest");
        props.put("crypto.encryptor","avro");
        return props;
      
    }
    private static String randGroupName(){
    	return "test_group_" + new Random().nextInt(100000);
    }
	//@Test
	public void testProducer() {	
		//TODO: Add more tests for wrong configs - no valid keys, etc.  
		
		initProducer();
		try{
		  ProducerRecord rec = new ProducerRecord<byte[], byte[]> (topic, null, TEST_STRING.getBytes());
		  for (int i =0; i< MSG_COUNT; i++){
		      producer.send(rec);		
		  }
		}catch (Exception e) {
				fail("Producer failed");
		} 
	}
	//@Test
	void  testConsumer(){
        try { 
		   consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				new ConsumerConfig(getConsumerConfig()));
        }catch (Exception e) {
			fail("failed to create a consumer with error" + e);
	    } 
        try{ 
        	
		   Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
           topicCountMap.put(topic, new Integer(1));
           VerifiableProperties vProps = new VerifiableProperties(getConsumerConfig());
           DefaultDecoder keyDecoder = new DefaultDecoder(vProps);
           KafkaSecureByteArrayDecoder valueDecoder = new KafkaSecureByteArrayDecoder(vProps);
           Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
      
           final KafkaStream stream = consumerMap.get(topic).get(0);
           ConsumerIterator<byte[], byte[]> it = stream.iterator();
           while (it.hasNext()){
             System.out.println("Consumer got: " + new String(it.next().message()));
             assertEquals("Should get the original string in the consumer",
        		   TEST_STRING, new String(it.next().message()));
           }
        }catch (Exception e) {
			fail("failed to  consume message stream, with error:" + e);
	    } 
       
	}
	//TODO: Rewrite using JUNIT
	public static void main(String [ ] args) {
		KafkaSecureProducerTest test = new KafkaSecureProducerTest();
		test.testProducer();
		test.testConsumer();
	}

}
