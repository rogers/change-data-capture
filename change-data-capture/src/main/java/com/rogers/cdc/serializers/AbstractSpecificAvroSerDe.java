package com.rogers.cdc.serializers;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.Map;

import org.apache.avro.Schema;

import com.rogers.cdc.api.mutations.Mutation;


/* AbstractSpecificAvroSerDe
 * A schema is generated for each table in runtime and is stored in a Schema Registry (where topic = table name)
 * If the tables schema changes, a new Avro schema will be generated and uploaded to the Schema Registry when the next operation is processed.
 * If the new schema is incompatible with the old schema (based on the Schema Registry compatibility settings) an exception be thrown
 *
 * Updates and Inserts are stored
 *
 * org.apache.kafka.connect.data is used as an intermediate representation to try to be compatible with Kafka Connect
 */
abstract public class AbstractSpecificAvroSerDe{
	protected AvroConverter converter; 
//	private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
	//protected AvroData avroData;
	protected boolean isKey;
	 
	AbstractSpecificAvroSerDe(){
		converter = new AvroConverter();
		//avroData = new AvroData(SCHEMAS_CACHE_SIZE_DEFAULT);
	 }
	AbstractSpecificAvroSerDe(SchemaRegistryClient schemaRegistry ){
		converter = new AvroConverter(schemaRegistry);
	 }
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.isKey = isKey;
		converter.configure(configs, isKey);
	}
	 // TODO: Need a real mock schemare registry
	//  INterface may depend on wheather we want to be able to evolve schemas,  
	 //protected  Schema getSchema(Mutation op){
		// return avroData.fromConnectSchema(op.getTable().getSchema()); 
	 //}
	/* protected  Schema getSchemaById(short id){
		 return schema; 
	 }
	 protected  String getSchemaSubject(Mutation op){
		 return "test";  //TODO
	 }
	 protected  Short getSchemaId(String topic, Schema schema){
		 return 1; //TODO
	 };*/
}