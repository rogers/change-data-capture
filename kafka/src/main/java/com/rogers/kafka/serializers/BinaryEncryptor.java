package com.rogers.kafka.serializers;

import com.rogers.kafka.crypto.key.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;


public class BinaryEncryptor extends Encryptor {

    public static final int SECRET_KEY_LENGTH_BITS = 1024;

    final private static Logger logger = LoggerFactory.getLogger(BinaryEncryptor.class);


    public BinaryEncryptor(KeyProvider _provider) {
        super(_provider);
    }

    @Override
    public byte[] encrypt(byte[] messageBytes, String[] public_keys) throws Exception {
        //TODO: add support for multiple keys
        assert (public_keys.length == 1);
        String public_key = public_keys[0];

        // Encrypt the message with a new symmetric key.
        SymmetricEncryptor symmetricEncryptor = new SymmetricEncryptor();
        byte[] encryptedMessage = symmetricEncryptor.encrypt(messageBytes);

        PublicKey publicKey = AsymmetricKeyReader.readPublicKey(provider.getCurrentPublicKey(public_key).getMaterial());
        // Encrypt the symmetric key with the public key.
        AsymmetricEncryptor asymmetricEncriptor = new AsymmetricEncryptor(publicKey);
        byte[] secretKeyBytes = symmetricEncryptor.getKey().getEncoded();
        byte[] encryptedSecretKey = asymmetricEncriptor.encrypt(secretKeyBytes);

        // Encrypt the symmetric key initialization vector with the public key.
        byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
        byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);

        EncryptedMessage msg = new EncryptedMessage(encryptedMessage, encryptedSecretKey, encryptedIV, public_key);

        return msg.toByteArray();

    }

    @Override
    public byte[] decrypt(byte[] payload) throws Exception {
        EncryptedMessage msg = new EncryptedMessage(payload);
        return decryptImp(msg.getMessage(), msg.getKey(), msg.getIV(), msg.getKeyName());
    }
}