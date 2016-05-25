package com.rogers.kafka.serializers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

// TODO: Should this by part of Encryptor? What if Cipher in Encryptor changes? 
public class EncryptedMessage {

    public static final int ENC_KEY_LENGTH_BITS = BinaryEncryptor.SECRET_KEY_LENGTH_BITS / 8;
    public static final int ENC_IV_LENGTH_BYTES = BinaryEncryptor.SECRET_KEY_LENGTH_BITS / 8;

    final private static Logger logger = LoggerFactory.getLogger(EncryptedMessage.class);

    private static final int KEY_NAME_LENGTH = 16;

    private byte[] encryptedMessage;
    private byte[] encryptedSecretKey;
    private byte[] encryptedIV;
    private String keyName;

    public EncryptedMessage(byte[] _encryptedMessage, byte[] _encryptedSecretKey, byte[] _encryptedIV, String _keyName) {
        assert _encryptedSecretKey.length == ENC_KEY_LENGTH_BITS : "Wrong encryptedSecretKey length:" + _encryptedSecretKey.length;
        assert _encryptedIV.length == ENC_IV_LENGTH_BYTES : "Wrong encryptedIV length: " + _encryptedIV.length;
        logger.debug("Enc Secret Key = " + _encryptedSecretKey);
        encryptedMessage = _encryptedMessage;
        encryptedSecretKey = _encryptedSecretKey;
        encryptedIV = _encryptedIV;
        keyName = _keyName;
        assert (keyName.length() < KEY_NAME_LENGTH);
    }

    public EncryptedMessage(byte[] payload) {
        encryptedSecretKey = Arrays.copyOfRange(payload, 0, ENC_KEY_LENGTH_BITS);
        encryptedIV = Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES);
        keyName = new String(Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES + KEY_NAME_LENGTH)).replaceAll("\\s+", "");
        encryptedMessage = Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES + KEY_NAME_LENGTH, payload.length);
        //encryptedMessage =  Arrays.copyOfRange(payload, ENC_KEY_LENGTH_BITS + ENC_IV_LENGTH_BYTES , payload.length);
    }

    private static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    public byte[] getMessage() {
        return encryptedMessage;
    }

    public byte[] getKey() {
        return encryptedSecretKey;
    }

    public byte[] getIV() {
        return encryptedIV;
    }

    public String getKeyName() {
        return keyName;
    }

    public byte[] toByteArray() throws IOException {
        //TODO: add version number
        //TODO: support for multiple <key, key name> pairs
        //TODO: use size + payload for storing key names?
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(this.getKey());
        out.write(this.getIV());
        out.write(padRight(this.getKeyName(), KEY_NAME_LENGTH).getBytes());
        System.out.print("Key lenght = " + padRight(this.getKeyName(), KEY_NAME_LENGTH).getBytes().length);
        out.write(this.getMessage());
        return out.toByteArray();
    } // pad with " " to the left to the given length (n)


}
