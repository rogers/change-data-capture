package com.rogers.cdc.kafka.serializers;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.serializers.GenericAvroMutationDeserializer;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;


/*
 * A wrapper for the old Kafka Consumer/Producer Scala API that was using Encoder/Decoder instead of Serializer/Deserializer
* */
public class KafkaGenericAvroMutationDecoder extends GenericAvroMutationDeserializer
        implements Decoder<Mutation> {
    private Decoder<byte[]> firstDeserializer;

    /**
     * Constructor used for testing.
     */
    public KafkaGenericAvroMutationDecoder(Decoder<byte[]> _firstDeserializer) {
        //TODO: Move to AbstractAvroDeserilaizer
        if (_firstDeserializer == null) {
            firstDeserializer = new DefaultDecoder(null);
            //TODO: Should get this from config - look at KafkaProducer code...(in Kafka src)
        } else {
            firstDeserializer = _firstDeserializer;
        }

    }

    /**
     * Constructor used by Kafka consumer.
     */
    public KafkaGenericAvroMutationDecoder(VerifiableProperties props) {
        //TODO Parase Props file....
        firstDeserializer = new DefaultDecoder(null);
    }


    public void configure(VerifiableProperties props) {
    }


    @Override
    public Mutation fromBytes(byte[] data) {
        byte[] bytes;
        if (firstDeserializer != null) {
            bytes = firstDeserializer.fromBytes(data);
        } else {
            bytes = data;
        }
        return deserializeImp(bytes);
    }


}

