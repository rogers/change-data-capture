package com.rogers.kafka.crypto.key;

import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A Key Provider for simple testings of having multiple keys
 * Just put a public-private key pair in the project path and specify the key names bellow
 * It doesn't do any key versioning, just returns the  public and private keys.
 *
 * @author eugene.miretsky
 */


public class ConfigKeyProvider extends KeyProvider {
    // TODO: create an AbstractConfig
    private static final String PRIVATE_KEY_FILE_CONFIG = "crypto.key.private.file";
    private static final String PRIVATE_KEY_NAME_CONFIG = "crypto.key.private.name";
    private static final String PRIVATE_KEY_NAME_DEFAULT = "test_key";
    private static final String PUBLIC_KEY_FILE_CONFIG = "crypto.key.public.file";
    private static final String PUBLIC_KEY_NAME_CONFIG = "crypto.key.public.name";
    private static final String PUBLIC_KEY_NAME_DEFAULT = "test_key";
    private boolean have_public_key = false;
    private boolean have_private_key = false;
    private byte[] current_public_key;
    private String current_public_key_name;
    private byte[] current_private_key;
    private String current_private_key_name;

    public ConfigKeyProvider(Map<?, ?> configs) {
        super(configs);
        String file;
        current_public_key_name = PUBLIC_KEY_NAME_DEFAULT;
        current_private_key_name = PRIVATE_KEY_NAME_DEFAULT;
        Object public_key = configs.get(PUBLIC_KEY_FILE_CONFIG);
        if (public_key != null) {
            file = (String) public_key;
            try {
                current_public_key = Files.readAllBytes(Paths.get(file));
                have_public_key = true;
            } catch (IOException e) {
                throw new ConfigException("Can't find  public_key file " + file, e);
            }
        }
        Object private_key = configs.get(PRIVATE_KEY_FILE_CONFIG);
        if (private_key != null) {
            file = (String) private_key;
            try {
                current_private_key = Files.readAllBytes(Paths.get(file));
                have_private_key = true;
            } catch (IOException e) {
                throw new ConfigException("Can't find  private_key file " + file, e);
            }
        }
        if (!have_public_key && !have_private_key) {
            throw new ConfigException("Need at least 1 key set up"); //Probably want a custom exception
        }

    }

    @Override
    public KeyVersion getCurrentPublicKey(String name) throws IOException {
        // Just returning version = 1 for all
        if (name == current_public_key_name && have_public_key) {
            return new KeyVersion(name, buildVersionName(name, 1), current_public_key);
        } else {
            throw new IOException("Can't find current key for " + name);
        }
    }

    public String[] getAllPublicKeys() throws IOException {
        String arr[] = {"key"};
        return arr;
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
        // Just returning version = 1 for all
        if (versionName == current_private_key_name) {
            return new KeyVersion(getBaseName(versionName), versionName, current_private_key);
        } else {
            return null;
        }
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name) throws IOException {
        List<KeyVersion> list = new ArrayList<KeyVersion>();
        list.add(getKeyVersion(current_public_key_name));
        list.add(getKeyVersion(current_private_key_name));
        return list;
    }

    @Override
    public void flush() throws IOException {
        //NOP
    }

}
