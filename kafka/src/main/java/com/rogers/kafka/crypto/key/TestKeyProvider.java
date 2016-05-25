package com.rogers.kafka.crypto.key;

import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * A Key Provider for simple testings
 * Just put a public-private key pair in the project path and specify the key names bellow
 *
 * @author eugene.miretsky
 */
public class TestKeyProvider extends KeyProvider {
    private static final String PUBLIC_KEY_FILENAME = "public_key.der";
    private static final String PRIVATE_KEY_FILENAME = "private_key.der";
    private static final String PUBLIC_KEY_FILENAME2 = "public_key2.der";
    private static final String PRIVATE_KEY_FILENAME2 = "private_key2.der";
    public static String KEY_NAME = "key";
    public static String KEY_NAME2 = "key2";
    private byte[] current_public_key;
    private byte[] current_private_key;
    private byte[] current_public_key2;
    private byte[] current_private_key2;

    public TestKeyProvider() {
        super(null);

        try {
            current_private_key = readAllBytes(PRIVATE_KEY_FILENAME);
            current_public_key = readAllBytes(PUBLIC_KEY_FILENAME);
            current_private_key2 = readAllBytes(PRIVATE_KEY_FILENAME2);
            current_public_key2 = readAllBytes(PUBLIC_KEY_FILENAME2);
        } catch (Exception e) {
            throw new ConfigException("Cannot find key files: " + e);
        }
    }

    private static byte[] readAllBytes(String filename) throws Exception {
        return Files.readAllBytes(Paths.get(filename));
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
        // Just returning version = 1 for all
        if (versionName.equals(KEY_NAME)) {
            return new KeyVersion(getBaseName(versionName), versionName, current_private_key);
        } else if (versionName.equals(KEY_NAME2)) {
            return new KeyVersion(getBaseName(versionName), versionName, current_private_key2);
        } else {
            throw new IOException("Can't get key version " + versionName + " from TestKeyProvider");
        }
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name) throws IOException {
        List<KeyVersion> list = new ArrayList<KeyVersion>();
        list.add(getKeyVersion(name));
        return list;
    }

    @Override
    public void flush() throws IOException {
        //NOP
    }

    @Override
    public KeyVersion getCurrentPublicKey(String name) throws IOException {
        if (name.equals(KEY_NAME)) {
            return new KeyVersion(name, buildVersionName(name, -1), current_public_key);
        } else if (name.equals(KEY_NAME2)) {
            return new KeyVersion(name, buildVersionName(name, -1), current_public_key2);
        } else {
            throw new IOException("Can't get current public key " + name + " from TestKeyProvider");
        }
    }

    @Override
    public String[] getAllPublicKeys() throws IOException {
        String arr[] = {KEY_NAME, KEY_NAME2};
        return arr;
    }

}


