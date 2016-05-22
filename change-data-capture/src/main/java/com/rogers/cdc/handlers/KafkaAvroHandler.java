package com.rogers.cdc.handlers;

import java.io.IOException;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.api.serializer.MutationSerializer;
import com.rogers.cdc.kafka.KafkaUtil;
import com.rogers.cdc.serializers.SpecificAvroMutationSerializer;

/**
 * A Kafka handler that serializes generic CDC operation (Op) to Avro objects
 * and sends them to Kafka Expects a MutationMapper to Map Generic Ops to
 * Mutations (which are used internally as a generic Op representation )
 * 
 * For example: A GoldenGate (GG) Adapter would instantiate this class with an
 * MutationMapper that maps GG Ops to Mutations and then call processOp on a GG
 * Op to send it to Kafka.
 */
public class KafkaAvroHandler<Op, Table, OpMapper extends MutationMapper<Op, Table>>
		extends KafkaHandler<Op, Table, OpMapper> {
	public static byte PROTO_MAGIC_V0 = 0x0;

	final private static Logger logger = LoggerFactory
			.getLogger(KafkaAvroHandler.class);

	MutationSerializer valSerialiazer;
	MutationSerializer keySerialiazer;

	public KafkaAvroHandler(OpMapper _opMapper, String configFile) {
		super(_opMapper, configFile);
		valSerialiazer = new SpecificAvroMutationSerializer();
		valSerialiazer.configure(
				new AbstractConfig(new ConfigDef(), config).originals(), false);
		keySerialiazer = new SpecificAvroMutationSerializer();
		keySerialiazer.configure(
				new AbstractConfig(new ConfigDef(), config).originals(), true);
		
	}

	@Override
	public void processOp(Op op) {
		try {
			logger.debug("Start processing {}", op);
			Mutation mutation = opMapper.toMutation(op);
			String topic = getSchemaSubject(mutation);
			byte[] val = valSerialiazer.serialize(topic, mutation);
			byte[] key = keySerialiazer.serialize(topic, mutation);
			logger.info("KafkaHandler: Send Message to topic: {} " , topic);
			send(topic, key, val);
		} catch (IOException e) {
			throw new RuntimeException(" Failed to map op to Mutation ", e);
		} catch (RuntimeException e) {
			throw new RuntimeException(" Failed to serialize or send mutation",
					e);
		}
	}

	protected String getSchemaSubject(Mutation op) {
		return KafkaUtil.genericTopic(op);
	}

}
