package com.rogers.cdc.kafka.serializers;

import org.apache.kafka.connect.data.Struct;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.serializers.*;

/*
 * A wrapper for the old Kafka Consumer/Producer Scala API that was using Encoder/Decoder instead of Serializer/Deserializer
* */
public class KafkaSpecificAvroMutationDecoder extends
		SpecificAvroMutationDeserializer implements Decoder<Struct> {
	private Decoder<byte[]> firstDeserializer;

	/**
	 * Constructor used for testing.
	 */
	public KafkaSpecificAvroMutationDecoder(Decoder<byte[]> _firstDeserializer) {
		// TODO: Move to AbstractAvroDeserilaizer
		if (_firstDeserializer == null) {
			firstDeserializer = new DefaultDecoder(null);
			// TODO: Should get this from config - look at KafkaProducer
			// code...(in Kafka src)
		} else {
			firstDeserializer = _firstDeserializer;
		}

	}

	/**
	 * Constructor used by Kafka consumer.
	 */
	public KafkaSpecificAvroMutationDecoder(VerifiableProperties props) {
		// TODO Parase Props file....
		firstDeserializer = new DefaultDecoder(null);
	}

	/*public void configure(VerifiableProperties props) {
	}*/

	@Override
	public Struct fromBytes(byte[] data) {
		byte[] bytes;
		if (firstDeserializer != null) {
			bytes = firstDeserializer.fromBytes(data);
		} else {
			bytes = data;
		}
		// TODO: Passing a null topic is a hack. It never gets used in the current Confluent deserializer so it works, but this could change...
		// s
		return deserializeImpl(null, bytes);
	}

}
