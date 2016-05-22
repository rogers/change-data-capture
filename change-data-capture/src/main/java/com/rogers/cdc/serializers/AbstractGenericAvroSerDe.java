package com.rogers.cdc.serializers;
import java.io.Serializable;

import org.apache.avro.Schema;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.avro.InsertMutation;

// Deprecated!!!! May or may not work

/* AbstractGenericAvroSerDe
 * Uses a Generic Avro Schema to store changes for all tables
 * The schema is defined in InsertMutation
 * Basically, all the inserted/updated columns are stored in a map of column_name -> string value
 * Should be used if there is no Schema Registry to stores the schemas for each table
 */
abstract public class AbstractGenericAvroSerDe{
	 protected   static final  byte  PROTO_MAGIC_V0 = 0x0; 
	 protected static final int idSize = 2;
	 protected static final int opTypeSize = 1;
	 
			 
	 
	Schema schema = null; //TODO
	 
	AbstractGenericAvroSerDe(){
		//TODO
			 try {
				  schema =  InsertMutation.getClassSchema();
				}
				catch(Exception e){
					 throw new RuntimeException("Couldn't find Avro file" + e);
					
				}
	 }
	 // TODO: Need a real mock schemare registry
	//  INterface may depend on wheather we want to be able to evolve schemas,  
	 protected  Schema getSchema(Mutation op){
		 return schema; 
	 }
	 protected  Schema getSchemaById(short id){
		 return schema; 
	 }
	 protected  String getSchemaSubject(Mutation op){
		 return "test";  //TODO
	 }
	 protected  Short getSchemaId(String topic, Schema schema){
		 return 1; //TODO
	 };
}
