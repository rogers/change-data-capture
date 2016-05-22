package com.rogers.cdc.api.serializer;



import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.rogers.cdc.api.mutations.Mutation;

/**
 *
 * 
 */


public interface MutationDeserializer extends Deserializer<Mutation> {

    /**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     *
    public void configure(Map<String, ?> configs);

    /**
     * @param Mutation
     * @return serialized bytes
     *
    public Mutation deserialize( String topic, byte[] payload);


    /**
     * Close this deserializer.
     *
    @Override
    public void close();
    */
}