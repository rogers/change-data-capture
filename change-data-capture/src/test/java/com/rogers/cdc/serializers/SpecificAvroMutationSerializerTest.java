package com.rogers.cdc.serializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.joda.time.DateTime;
import org.junit.Test;

import scala.collection.immutable.List;

import com.rogers.cdc.api.mutations.Column;
import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.api.serializer.MutationDeserializer;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.kafka.KafkaUtil;

//import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class SpecificAvroMutationSerializerTest {
	private SchemaRegistryClient schemaRegistry;

	private final String SCHEMA_REGISTRY_URL = "http://ec2-52-91-2-237.compute-1.amazonaws.com:8081";

	public SpecificAvroMutationSerializerTest() {

	}

	@Test
	public void testSpecificAvroMutationSerializerWithMockRegistry() {
		schemaRegistry = new MockSchemaRegistryClient();
		MutationSerializer keySerializer = new SpecificAvroMutationSerializer(
				schemaRegistry);
		MutationSerializer valSerializer = new SpecificAvroMutationSerializer(
				schemaRegistry);
		SpecificAvroMutationDeserializer keyDeserializer = new SpecificAvroMutationDeserializer(
				schemaRegistry);
		SpecificAvroMutationDeserializer valDeserializer = new SpecificAvroMutationDeserializer(
				schemaRegistry);

		keySerializer.configure(Collections.singletonMap("schema.registry.url",
				"http://fake-url"), true);
		valSerializer.configure(Collections.singletonMap("schema.registry.url",
				"http://fake-url"), false);
		keyDeserializer.configure(Collections.singletonMap(
				"schema.registry.url", "http://fake-url"), true);
		valDeserializer.configure(Collections.singletonMap(
				"schema.registry.url", "http://fake-url"), false);

		testSpecificAvroMutationSerializerImpl(keySerializer, valSerializer,
				keyDeserializer, valDeserializer);

	}

	/*
	 * @Test public void testSpecificAvroMutationSerializerWithRealRegistry() {
	 * MutationSerializer serializer = new SpecificAvroMutationSerializer();
	 * SpecificAvroMutationDeserializer deserializer = new
	 * SpecificAvroMutationDeserializer();
	 * 
	 * serializer.configure(Collections.singletonMap("schema.registry.url",
	 * SCHEMA_REGISTRY_URL),true );
	 * deserializer.configure(Collections.singletonMap("schema.registry.url",
	 * SCHEMA_REGISTRY_URL), true);
	 * 
	 * testSpecificAvroMutationSerializerImpl(serializer, deserializer );
	 * 
	 * }
	 */
	// Private
	private void testSpecificAvroMutationSerializerImpl(
			MutationSerializer keySerializer, MutationSerializer valSerializer,
			SpecificAvroMutationDeserializer keyDeserializer,
			SpecificAvroMutationDeserializer valDeserializer) {
		
		Schema schema = SchemaBuilder.struct()
				.field("int8", Table.SQL_OPTIONAL_INT8_SCHEMA)
				.field("int16", Table.SQL_OPTIONAL_INT16_SCHEMA)
				.field("int32", Table.SQL_OPTIONAL_INT32_SCHEMA)
				.field("int32b", Table.SQL_OPTIONAL_INT32_SCHEMA)
				.field("int64", Table.SQL_OPTIONAL_INT64_SCHEMA)
				.field("float32", Table.SQL_OPTIONAL_FLOAT32_SCHEMA)
				.field("float64", Table.SQL_FLOAT64_SCHEMA)
				.field("boolean", Table.SQL_OPTIONAL_BOOLEAN_SCHEMA)
				.field("string", Table.SQL_OPTIONAL_STRING_SCHEMA)
				.field("timeStamp", Table.SQL_TIMESTAMP_SCHEMA)
				.field("timeStamp_opt", Table.SQL_OPTIONAL_TIMESTAMP_SCHEMA)
				.field("bytes", Table.SQL_OPTIONAL_BYTES_SCHEMA).build();
		Table table = new Table("testSchema", "testTable");
		table.setSchema(schema, Arrays.asList("int32"));
		Struct insertRow = new Struct(schema)
				.put("int8", Table.getSQLSchemaField(schema, "int8", (byte) 12))
				.put("int16",
						Table.getSQLSchemaField(schema, "int16", (short) 12))
				.put("int32", Table.getSQLSchemaField(schema, "int32", 12))
				.put("int32b", Table.getSQLSchemaField(schema, "int32b", null))
				// Testing SQL null
				.put("int64",
						Table.getSQLSchemaField(schema, "int64", (long) 12))
				.put("float32",
						Table.getSQLSchemaField(schema, "float32", 12.f))
				.put("float64", Table.getSQLSchemaField(schema, "float64", 12.))
				.put("boolean",
						Table.getSQLSchemaField(schema, "boolean", true))
				.put("string",
						Table.getSQLSchemaField(schema, "string", "foobar"))
				// .put("timeStamp", Table.getSQLSchemaField( schema,
				// "timeStamp", new Timestamp(DateTime.now().getMillis())))
				// //TODO: Need to test Timestamp, not Date
				.put("timeStamp",
						Table.getSQLSchemaField(schema, "timeStamp", new Date(
								DateTime.now().getMillis())))
				.put("timeStamp_opt",
						Table.getSQLSchemaField(schema, "timeStamp_opt", null))
				.put("bytes",
						Table.getSQLSchemaField(schema, "bytes",
								ByteBuffer.wrap("foobar".getBytes())));
		Struct updateRow = new Struct(schema)
				.put("int32", Table.getSQLSchemaField(schema, "int32", 12))
				.put("float64", Table.getSQLSchemaField(schema, "float64", 12.))
				.put("string",
						Table.getSQLSchemaField(schema, "string", "foobar"));
		Struct deleteKey = new Struct(schema).put("int32",
				Table.getSQLSchemaField(schema, "int32", 12));// Delete still
																// need to have
																// key info
		Struct pkUpdateKey = new Struct(schema).put("int32",
				Table.getSQLSchemaField(schema, "int32", 12));
		Struct pkUpdateRow = new Struct(schema).put("int32",
				Table.getSQLSchemaField(schema, "int32", 13));

		UpdateMutation updateM = new UpdateMutation(table,
				Row.fromStruct(updateRow));
		InsertMutation insertM = new InsertMutation(table,
				Row.fromStruct(insertRow));
		DeleteMutation deleteM = new DeleteMutation(table,
				Row.fromStruct(deleteKey));
		PkUpdateMutation pkUpdateM = new PkUpdateMutation(table,
				Row.fromStruct(pkUpdateKey), Row.fromStruct(pkUpdateRow));

		byte[] keyOutput;
		byte[] valOutput;
		Struct keyRes;
		Struct valRes;
		Mutation res;

		String topic = KafkaUtil.genericTopic(table);

		// try {
		keyOutput = keySerializer.serialize(topic, insertM);
		valOutput = valSerializer.serialize(topic, insertM);
		keyRes = keyDeserializer.deserialize(topic, keyOutput);
		valRes = valDeserializer.deserialize(topic, valOutput);
		res = Mutation.fromStructKeyVal(KafkaUtil.topicToDbName(topic),
				KafkaUtil.topicToTableName(topic), keyRes, valRes);
		System.out.println("Insert: " + insertM);
		assertEquals("Insert Mutation: Results should be the same as orignal",
				insertM, res);

		keyOutput = keySerializer.serialize(topic, updateM);
		valOutput = valSerializer.serialize(topic, updateM);
		keyRes = keyDeserializer.deserialize(topic, keyOutput);
		valRes = valDeserializer.deserialize(topic, valOutput);
		res = Mutation.fromStructKeyVal(KafkaUtil.topicToDbName(topic),
				KafkaUtil.topicToTableName(topic), keyRes, valRes);
		System.out.println("Insert: " + updateM);
		assertEquals("Update Mutation: Results should be the same as orignal",
				updateM, res);

		keyOutput = keySerializer.serialize(topic, deleteM);
		valOutput = valSerializer.serialize(topic, deleteM);
		keyRes = keyDeserializer.deserialize(topic, keyOutput);
		valRes = valDeserializer.deserialize(topic, valOutput);
		res = Mutation.fromStructKeyVal(KafkaUtil.topicToDbName(topic),
				KafkaUtil.topicToTableName(topic), keyRes, valRes);
		System.out.println("Delete: " + deleteM);
		assertEquals("deleteM Mutation: Results should be the same as orignal",
				deleteM, res);

		keyOutput = keySerializer.serialize(topic, pkUpdateM);
		valOutput = valSerializer.serialize(topic, pkUpdateM);
		keyRes = keyDeserializer.deserialize(topic, keyOutput);
		valRes = valDeserializer.deserialize(topic, valOutput);
		res = Mutation.fromStructKeyVal(KafkaUtil.topicToDbName(topic),
				KafkaUtil.topicToTableName(topic), keyRes, valRes);
		System.out.println("Insert: " + pkUpdateM);
		assertEquals(
				"pkUpdateM Mutation: Results should be the same as orignal",
				pkUpdateM, res);

		// TODO: PkDoesn't work yet
		/*
		 * output = serializer.serialize(topic, pkUpdateM); res =
		 * deserializer.deserialize(topic, output);
		 * System.out.println("pkUpdateM: " + pkUpdateM);
		 * assertEquals("UpdatePk Mutation: Results should be the same as orignal"
		 * , pkUpdateM, res); //}catch (Exception e){
		 * //fail("Unexpecte expception: " + e); //}
		 */

	}
}
