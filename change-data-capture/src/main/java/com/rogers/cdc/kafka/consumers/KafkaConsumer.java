package com.rogers.cdc.kafka.consumers;

//import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.rogers.goldengate.kafka.KafkaUtil;
//import com.rogers.kafka.Producer;





import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.Consumer;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;



public abstract class KafkaConsumer<Key, Val> {
	private ConsumerConnector consumer;
	private KafkaStream stream;
	
	 kafka.consumer.ConsumerConfig bla;
	
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaConsumer.class);
	
	abstract void procEvent(Val event);
	
	public KafkaConsumer(final String topic, final String zkConnect, final String groupId, Decoder<Val> decoder){
		
		 
		 consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
					new ConsumerConfig(getConsumerConfig(zkConnect, groupId)));
		 
		 Map<String, Integer> topicCountMap = new HashMap(){{
             put(topic, new Integer(1));
		 }};
		 Map<String, List<KafkaStream<byte[], Val>>> consumerMap = consumer.createMessageStreams(topicCountMap, new DefaultDecoder(null), decoder);
		 stream = consumerMap.get(topic).get(0);
	}
	public void start(){
        ConsumerIterator<Key, Val> it = stream.iterator();
        while (it.hasNext()){
          try {
        	  Val msg = it.next().message(); 
             logger.debug("Consumer got: []" , msg);
             procEvent(msg);
          }catch (Exception e){
        	  //TODO: Need better logging than this..... Don't want to lose message
        	  logger.error("Failed deserializing or processing message: with error" +  e);
          }     
        }
	}
    
	private  Properties getConsumerConfig(String zkConnect, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","smallest");
        props.put("crypto.encryptor","avro");
        return props;
      
    }

}
