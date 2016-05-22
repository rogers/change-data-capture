package com.rogers.cdc.kafka.serializers;

import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.serializers.GenericAvroMutationDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

//TODO: Remove this class? MutationDeserialize and Deserializer are the same now
/* Wraps  a MutationDeserializer in  Deserializer interface */
public class KafkaGenericAvroMutationDeserializer extends GenericAvroMutationDeserializer
        implements Deserializer<Mutation> {
    private Deserializer<byte[]> firstDeserializer;

    KafkaGenericAvroMutationDeserializer(Deserializer<byte[]> _firstDeserializer) {
        if (_firstDeserializer == null) {
            firstDeserializer = new ByteArrayDeserializer();
            //TODO: Should get this from config - look at KafkaProducer code...(in Kafka src)
        } else {
            firstDeserializer = _firstDeserializer;
        }

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Mutation deserialize(String topic, byte[] data) {
        byte[] bytes;
        if (firstDeserializer != null) {
            bytes = firstDeserializer.deserialize(topic, data);
        } else {
            bytes = data;
        }
        return deserializeImp(bytes);
    }

    @Override
    public void close() {

    }


}
