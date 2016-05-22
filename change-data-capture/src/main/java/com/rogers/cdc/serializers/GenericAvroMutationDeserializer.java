package com.rogers.cdc.serializers;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.exceptions.SerializationException;


public class GenericAvroMutationDeserializer extends AbstractGenericAvroSerDe implements MutationDeserializer {
	final private static Logger logger = LoggerFactory
			.getLogger(GenericAvroMutationDeserializer.class);
	 
	transient private final DecoderFactory decoderFactory = DecoderFactory.get();

	 //TODO: Maybe we should return just the GenericRecord?
    protected Mutation deserializeImp( byte[]  payload){
    	 ByteBuffer buffer = getByteBuffer(payload);
    	 byte type = buffer.get();
    	 short schemaId = buffer.getShort();
    	 int length = buffer.limit() - 1 - idSize - opTypeSize;
    	 Schema schema = getSchemaById(schemaId);
    	 
    	 try { 

           int start = buffer.position() + buffer.arrayOffset();
           //TODO: Should probably parametrize for GenericRecord. 
           // OR create something like https://github.com/mardambey/mypipe/blob/7900bc6bafbff44c268c1f22efcccff42fa913e1/mypipe-avro/src/main/scala/mypipe/avro/AvroRecordMutationDeserializer.scala
           DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);;
           GenericRecord object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));// TODO: Do we want to reuse a decoder and Datum?

          // result = object;
           Mutation mutation = toMutation(object, type);
           return mutation; 
    	 
         } catch (IOException e) {
            throw new SerializationException("Error deserializing Avro message for id " + schemaId, e);
        
         } catch (RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
           throw new SerializationException("Error deserializing Avro message for schamaid " + schemaId + "with error: " ,  e);
          }
         
        
    }
    private Row createRows(GenericRecord rec){
    	Row row = new Row();
    	Map<String,String> strings= (Map<String,String>) rec.get("strings");
    	for(Map.Entry<String, String> entry: strings.entrySet()) {  
    		 String name = entry.getKey(); 
    		 Serializable val = entry.getValue();
    		 row.addColumn(name, val);
    	}
    	return row;
    }
    private Mutation toMutation(GenericRecord rec, byte type){
    	String tableName =  rec.get("table").toString();
	   String schemaName = rec.get("schema").toString();
	   Table table = new Table(schemaName, tableName);
	   table.setSchema(null,null); // Don't really have either here... 
	  
	    Mutation mutation; 
	  
    	switch(type){
           case Mutation.InsertByte:
           {
        	    mutation =  new InsertMutation(table, createRows(rec));

        	   break;
           }
           case  Mutation.DeleteByte: {
        	   mutation =  new DeleteMutation(table, createRows(rec));
   	           break;
           }
           case Mutation.UpdateByte: 
           {
        	   mutation =  new UpdateMutation(table, createRows(rec));

        	   break;
           }
          // case Mutation.UpdatePKByte: 
        	//   mutation =  new PkUpdateMutation(table, createRows(rec));
        	  // break;
           default:
        	   throw new IllegalArgumentException("AvroDesirializer unknown op type");                                                                            
        }    
    	return mutation;
    }
    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != PROTO_MAGIC_V0) {
          throw new SerializationException("Unknown magic byte!");
        }
        return buffer;
      }
    private DatumReader getDatumReader(Schema writerSchema) {
       // if (false) {
         // return new SpecificDatumReader(writerSchema, getReaderSchema(writerSchema));
       // } else {
          return new GenericDatumReader(writerSchema);
        //}
      }
    
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO: Anything to be done here?
		
	}
	@Override
	public Mutation deserialize(String topic,byte[] payload) {
		
		return deserializeImp(payload);
	}
	@Override
	public void close() {
		// TODO: Anything to be done here?

	}

}