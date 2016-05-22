package com.rogers.cdc.kafka.consumers;

import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.*;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.kafka.serializers.KafkaGenericAvroMutationDecoder;
import com.rogers.cdc.serializers.GenericAvroMutationDeserializer;

abstract public class KafkaMutationAvroConsumer extends KafkaConsumer<byte[], Mutation> {
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaMutationAvroConsumer.class);
	
	public KafkaMutationAvroConsumer(final String topic, final String zkConnect, final String groupId){
		// TODO: The way we pass a Decoder is ugly
		super(topic, zkConnect,groupId, new KafkaGenericAvroMutationDecoder(new VerifiableProperties()));
	}
	//TODO: Maybe we should just return Avro objects? 
	@Override
    void procEvent(Mutation op) {
		switch(op.getType()){
        case INSERT:
        {
     	   InsertMutation mutation =  op.getMutation();
     	   this.processInsertOp(mutation);
     	   break;
        }
        case  DELETE: {
        	DeleteMutation mutation =  op.getMutation();
     	    this.processDeleteOp(mutation);
     	   break;
        }
        case UPDATE: 
        {
     	   UpdateMutation mutation =  op.getMutation();
     	    this.processUpdateOp(mutation);
     	   break;
        }
        case PKUPDATE: 
     	    this.processPkUpdateOp(op);
     	   break;
        default:
     	   logger.error("The operation type " + op.getType() + " on  operation: table=[" + op.getTableName() + "]" + "is not supported");
     	   throw new RuntimeException("KafkaMutationAvroConsumer: Unknown operation type");                                                                            
      }      
    }
    abstract protected void processInsertOp(InsertMutation op);
    abstract protected void processDeleteOp(DeleteMutation op);
    abstract protected void processUpdateOp(UpdateMutation op);
    abstract protected void processPkUpdateOp(Mutation op);

}
