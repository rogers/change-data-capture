package com.rogers.kafka.crypto.key;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

/**
 * A factory to create a list of KeyProviders based on the value provided in the
 * Configuration.
 */

//TODO: Create factory using service loader 
public class KeyProviderFactory {
    public static final String TEST = "test";
    public static final String CONFIG = "config";

    public static KeyProvider getKeyProvider(String name, Map<?, ?> configs) {

        KeyProvider provider;
        switch (name) {
            case TEST:
                provider = new TestKeyProvider();
                break;
            case CONFIG:
                provider = new ConfigKeyProvider(configs);
                break;
            default:
                throw new ConfigException("Invalid key provider");
        }
        return provider;
    }

}