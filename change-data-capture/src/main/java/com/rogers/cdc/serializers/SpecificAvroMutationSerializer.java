package com.rogers.cdc.serializers;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.RowMutation;
import com.rogers.cdc.api.serializer.MutationSerializer;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.avro.generic.GenericContainer;
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
//import io.confluent.kafka.serializers.NonRecordContainer;

public class SpecificAvroMutationSerializer extends AbstractSpecificAvroSerDe implements MutationSerializer{ 
	final private static Logger logger = LoggerFactory
			.getLogger(SpecificAvroMutationSerializer.class);

	  
	  public SpecificAvroMutationSerializer(){

	  }
	  public SpecificAvroMutationSerializer(SchemaRegistryClient schemaRegistry){
		  super(schemaRegistry);

	  }
	   
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {	
			super.configure(configs, isKey);
			
		}
	  @Override
		 public byte[] serialize(String topic, Mutation op) { 
		    op.validate();
		    if (isKey){
		    	return serializeKey(topic, op);
		    }else{
		    	return serializeVal(topic, op);
		    }
	    }
	     private byte[] serializeKey(String topic, Mutation op) {
	    	 Struct record = op.getKey();
	    	 byte[] bytes;

			    try{ 
			       bytes = converter.fromConnectData(topic, record.schema(), record);
			    }catch (Exception e){
	        	   logger.error(" KafkaAvroSerializer serialization error: " , e);
				    throw new SerializationException("Failed to serialze Avro object, with error: " , e);
			    
			    }
		     return bytes; 
	    	 
	     }
		 private byte[] serializeVal(String topic, Mutation op) {
			 
			 Struct record = getRecord(op);
			 byte[] bytes = null;
      	   
             if (record != null){
			    try{ 
			       bytes = converter.fromConnectData(topic, record.schema(), record);
			    }catch (Exception e){
	        	   logger.error(" KafkaAvroSerializer serialization error: " , e);
				 throw new SerializationException("Failed to serialze Avro object, with error: " , e);
			    
			    }
             }
		     return bytes; 

		 }
	 protected  Struct getRecord(Mutation op){
		    Struct record;
		    logger.debug("\t avroRecord()  ");
		    
	        switch(op.getType()){
	           case INSERT:
	           case UPDATE:
	           case PKUPDATE:
	           {
	        	   logger.debug("\t Insert/Update  ");
	        	   RowMutation mutation =  op.getMutation();
	        	   record = mutation.getVal();
	        	   break;
	        	 
	           }
	           case  DELETE: {  
	        	   record = null; 
	        	   break;
	           }         
	           default:
	        	   logger.error("The operation type " + op.getType() + " on  operation: table=[" + op.getTableName() + "]" + "is not supported");
	        	   throw new IllegalArgumentException("SpecificAvroMutationSerializer::addBody Unknown operation type");                                                                            
       }  
	        return record;
	    }

	@Override
	public void close() {
		//TODO

	}

}
