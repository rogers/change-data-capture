package com.rogers.kafka.crypto.key;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

/**
 * A factory to create a list of KeyProvider based on the path given in a
 * Configuration. It uses a service loader interface to find the available
 * KeyProviders and create them based on the list of URIs.
 */
//TODO: Create factory using service loader 
public class KeyProviderFactory {
    public static KeyProvider getKeyProvider(String name, Map<?, ?> configs) {

        KeyProvider provider;
        switch (name) {
            case "test":
                provider = new TestKeyProvider();
                break;
            case "config":
                provider = new ConfigKeyProvider(configs);
                break;
            default:
                throw new ConfigException("Invalid key provider");
        }
        return provider;
    }

}