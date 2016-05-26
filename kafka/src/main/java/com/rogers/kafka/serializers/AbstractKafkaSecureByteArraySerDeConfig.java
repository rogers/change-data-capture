package com.rogers.kafka.serializers;

import com.rogers.kafka.crypto.key.KeyProvider;
import com.rogers.kafka.crypto.key.KeyProviderFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Created by eugene.miretsky on 2016-05-25.
 */
public class AbstractKafkaSecureByteArraySerDeConfig extends AbstractConfig {

    public static final String KEY_PROVIDER_CONFIG = "crypto.key_provider";
    public static final String ENCRYPTOR_CONFIG = "crypto.encryptor";
    public static final String ENCRYPTOR_DOC = ""; //TODO
    public static final String KEY_PROVIDER_DEFAULT = KeyProviderFactory.TEST;
    public static final String ENCRYPTOR_DEFAULT = EncryptorFactory.BINARY;
    private static final String KEY_PROVIDER_DOC = ""; // TODO:

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef().define(KEY_PROVIDER_CONFIG, ConfigDef.Type.STRING, KEY_PROVIDER_DEFAULT, ConfigDef.Importance.HIGH, KEY_PROVIDER_DOC)
                .define(ENCRYPTOR_CONFIG, ConfigDef.Type.STRING, ENCRYPTOR_DEFAULT, ConfigDef.Importance.HIGH, ENCRYPTOR_DOC);
    }

    public Map<?, ?> originals;

    public AbstractKafkaSecureByteArraySerDeConfig(Map<?, ?> props) {
        super(CONFIG, props);
        originals = props;
    }
}
