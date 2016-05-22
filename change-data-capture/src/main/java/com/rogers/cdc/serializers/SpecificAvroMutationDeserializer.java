package com.rogers.cdc.serializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.RowMutation;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.exceptions.SerializationException;
import com.rogers.cdc.kafka.KafkaUtil;

public class SpecificAvroMutationDeserializer extends AbstractSpecificAvroSerDe
		implements Deserializer<Struct> {

	// private Deserializer<Object> deserializer;

	public SpecificAvroMutationDeserializer() {
	}

	public SpecificAvroMutationDeserializer(SchemaRegistryClient schemaRegistry) {
		super(schemaRegistry);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

		super.configure(configs, isKey);

	}
	
	@Override
	public Struct deserialize(String topic, byte[] payload) {
		return deserializeImpl(topic, payload);
	}
		
	protected Struct deserializeImpl(String topic, byte[] payload) {
		/*
		 * if (isKey){ return deserializeKey(topic, op); }else{ return
		 * deserializeVal(topic, op); }
		 */
		if (!isKey && payload == null){
			// A delete mutation... 
			return null;
		}
		try {
			SchemaAndValue res = converter.toConnectData(topic, payload);
			Schema schema = res.schema();
			Object val = res.value();
			Struct struct = (Struct) val;
			// Schema will be null for delete mutation vals
			if (schema != struct.schema()) {
				throw new SerializationException(
						"Object schema doesn't match given schema");
			}
			return struct;
		} catch (RuntimeException e) {
			throw new SerializationException(
					"Error deserializing Avro message  ", e);
		}

	}

	/*
	 * private Struct deserializeKey(String topic, byte[] payload) {
	 * 
	 * } private Struct deserializeVal(String topic, byte[] payload) { try {
	 * SchemaAndValue res = converter.toConnectData(topic, payload); Schema
	 * schema = res.schema(); Object val = res.value(); Struct struct = (Struct)
	 * val; // Schema will be null for delete mutation vals if (schema !=
	 * struct.schema()){ throw new
	 * SerializationException("Object schema doesn't match given schema"); }
	 * return struct; // return toMutation(topic, row); } catch
	 * (RuntimeException e) {
	 * 
	 * throw new SerializationException("Error deserializing Avro message  ",
	 * e); }
	 * 
	 * }
	 */

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	/*
	 * private Mutation toMutation(String topic, Struct valStruct){
	 * 
	 * Row row = Row.fromStruct(valStruct); String dbName =
	 * KafkaUtil.topicToDbName(topic); String tableName =
	 * KafkaUtil.topicToTableName(topic);
	 * 
	 * Schema schema = (valStruct == null) ? null :valStruct.schema(); Table
	 * table = new Table(dbName, tableName); //TODO: get pKeys from key
	 * table.setSchema(schema, null);
	 * 
	 * Mutation mutation; // TODO: Need key info here.... to //1) Get schea for
	 * delete //2) tell the diffrence between update and pkUpdate // Probably
	 * shouldn't be here... move it to Mutation? if (row.size() == 0){ mutation
	 * = new DeleteMutation(table);
	 * 
	 * }else if (row.size() == schema.fields().size()){ mutation = new
	 * InsertMutation(table, row); }else{ mutation = new UpdateMutation(table,
	 * row); }
	 * 
	 * 
	 * return mutation; }
	 */

}
