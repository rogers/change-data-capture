package com.rogers.kafka.serializers;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Created by eugene.miretsky on 2016-05-25.
 */
public class EncryptorConfig extends AbstractConfig {

    public static final String SYM_KEY_ALGORITHM_CONFIG = "crypto.sym.key.alg";
    public static final String SYM_KEY_ALGORITHM_DOC = ""; //TODO
    public static final String SYM_KEY_ALGORITHM_DEFAULT = "AES";

    public static final String SYM_KEY_LENGTH_CONFIG = "crypto.sym.key.length";
    public static final String SYM_KEY_LENGTH_DOC = ""; //TODO
    public static final String SYM_KEY_LENGTH_DEFAULT = "128";

    public static final String SYM_CIPHER_ALGORITHM_CONFIG = "crypto.sym.cipher.alg";
    public static final String SYM_CIPHER_ALGORITHM_DOC = ""; //TODO
    public static final String SYM_CIPHER_ALGORITHM_DEFAULT = "AES/CBC/PKCS5Padding";

    public static final String ASYM_KEY_ALGORITHM_CONFIG = "crypto.sym.key.alg";
    public static final String ASYM_KEY_ALGORITHM_DOC = ""; //TODO
    public static final String ASYM_KEY_ALGORITHM_DEFAULT = "RSA";

    public static final String ASYM_CIPHER_ALGORITHM_CONFIG = "crypto.asym.cipher.alg";
    public static final String ASYM_CIPHER_ALGORITHM_DOC = ""; //TODO
    public static final String ASYM_CIPHER_ALGORITHM_DEFAULT = "RSA/ECB/PKCS1Padding";


    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(SYM_KEY_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SYM_KEY_ALGORITHM_DEFAULT, ConfigDef.Importance.HIGH, SYM_KEY_ALGORITHM_DOC)
                .define(SYM_KEY_LENGTH_CONFIG, ConfigDef.Type.INT, SYM_KEY_LENGTH_DEFAULT, ConfigDef.Importance.HIGH, SYM_KEY_LENGTH_DOC)
                .define(SYM_CIPHER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, SYM_CIPHER_ALGORITHM_DEFAULT, ConfigDef.Importance.HIGH, SYM_CIPHER_ALGORITHM_DOC)
                .define(ASYM_KEY_ALGORITHM_CONFIG, ConfigDef.Type.STRING, ASYM_KEY_ALGORITHM_DEFAULT, ConfigDef.Importance.HIGH, ASYM_KEY_ALGORITHM_DOC)
                .define(ASYM_CIPHER_ALGORITHM_CONFIG, ConfigDef.Type.STRING, ASYM_CIPHER_ALGORITHM_DEFAULT, ConfigDef.Importance.HIGH, ASYM_CIPHER_ALGORITHM_DOC);
    }

    public EncryptorConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }
    public String symKeyrAlg(){
        return this.getString(SYM_KEY_ALGORITHM_CONFIG);
    }
    public Integer symKeyLength(){
        return this.getInt(SYM_KEY_LENGTH_CONFIG);
    }
    public String symCipherAlg(){
        return this.getString(SYM_CIPHER_ALGORITHM_CONFIG);
    }
    public String asymKeyrAlg(){
        return this.getString(ASYM_KEY_ALGORITHM_CONFIG);
    }
    public String asymCipherAlg(){
        return this.getString(ASYM_CIPHER_ALGORITHM_CONFIG);
    }
}
