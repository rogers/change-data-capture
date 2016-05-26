package com.rogers.kafka.serializers;

import com.rogers.kafka.crypto.key.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public abstract class Encryptor {

    protected static final String SECRET_KEY_ALGORITHM = "AES";
    final private static Logger logger = LoggerFactory.getLogger(Encryptor.class);
    protected KeyProvider provider;

    public Encryptor(KeyProvider _provider) {
        provider = _provider;

    }

    public byte[] encrypt(String message, String[] public_keys) throws Exception {
        return encrypt(message.getBytes(), public_keys);
    }

    public byte[] encrypt(String message, String public_key) throws Exception {
        return encrypt(message.getBytes(), public_key);
    }

    abstract public byte[] encrypt(byte[] messageBytes, String[] public_keys) throws Exception;

    public byte[] encrypt(byte[] messageBytes, String public_key) throws Exception {
        String[] arr = {public_key};
        return encrypt(messageBytes, arr);
    }

    abstract public byte[] decrypt(byte[] payload) throws Exception;

    public String decryptToString(byte[] payload) throws Exception {
        byte[] receivedMessageBytes = decrypt(payload);
        String receivedMessage = new String(receivedMessageBytes, "UTF8");
        return receivedMessage;
    }

    protected byte[] decryptImp(byte[] encryptedMessage, byte[] encryptedKey, byte[] encryptedIV, String keyName) throws Exception {

        PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(provider.getKeyVersion(keyName).getMaterial());
        AsymmetricDecryptor asymmetricDecryptor = new AsymmetricDecryptor(privateKey);

        byte[] receivedSecretKeyBytes = asymmetricDecryptor.decrypt(encryptedKey);


        SecretKey receivedSecretKey = new SecretKeySpec(receivedSecretKeyBytes, SECRET_KEY_ALGORITHM);
        // Decrypt the symmetric key initialization vector with the private key.
        byte[] receivedIVBytes = asymmetricDecryptor.decrypt(encryptedIV);
        IvParameterSpec receivedIV = new IvParameterSpec(receivedIVBytes);

        // Decrypt the message.
        SymmetricDecryptor symmetricDecryptor = new SymmetricDecryptor(receivedSecretKey, receivedIV);
        byte[] receivedMessageBytes = symmetricDecryptor.decrypt(encryptedMessage);

        // The message that was received.
        return receivedMessageBytes;
    }
}


class SymmetricEncryptor {

    public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
    //public static final int    KEY_LENGTH_BITS  = 256;
    //TODO Install Java Cryptography Extension to allow 256 bits
    public static final int KEY_LENGTH_BITS = 128;
    public static final int IV_LENGTH_BYTES = 16;                           // 256/8 = 32; however, iv must be 16 bytes long (TODO: why?).
    static final String KEY_ALGORITHM = "AES";
    final private static Logger logger = LoggerFactory.getLogger(SymmetricEncryptor.class);

    private SecretKey _key;
    private IvParameterSpec _iv;
    private Cipher _cipher;

    public SymmetricEncryptor() throws Exception {
        _key = generateSymmetricKey();

        _iv = generateInitializationVector();
        _cipher = newCipher(_key, _iv);
    }

    private static SecretKey generateSymmetricKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGORITHM);
        SecureRandom random = new SecureRandom();
        generator.init(KEY_LENGTH_BITS, random);
        logger.debug("Generate Key");
        return generator.generateKey();
    }

    private static IvParameterSpec generateInitializationVector() {
        SecureRandom random = new SecureRandom();
        // TODO: what's going on here?
        return new IvParameterSpec("beb6f54c39089a9d".getBytes());
    }

    private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception {

        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, symmetricKey, iv);
        return cipher;
    }

    public byte[] encrypt(byte[] clearMessage) throws Exception {
        return _cipher.doFinal(clearMessage);
    }

    public SecretKey getKey() {
        return _key;
    }

    public IvParameterSpec getInitializationVector() {
        return _iv;
    }
}


//
// Decrypts byte arrays using a symmetric key.
//
class SymmetricDecryptor {
    public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

    private Cipher _cipher;

    public SymmetricDecryptor(SecretKey key, IvParameterSpec iv) throws Exception {
        _cipher = newCipher(key, iv);
    }

    private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, symmetricKey, iv);
        return cipher;
    }

    public byte[] decrypt(byte[] encryptedMessage) throws Exception {
        return _cipher.doFinal(encryptedMessage);
    }
}


//
// Encrypts byte arrays using an asymmetric key pair.
//
class AsymmetricEncryptor {
    //TODO: Put padding back
    public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";
    //public static final String CIPHER_ALGORITHM = "RSA/ECB/NoPadding";


    private Cipher _cipher;

    public AsymmetricEncryptor(PublicKey publicKey) throws Exception {
        _cipher = newCipher(publicKey);
    }

    private static Cipher newCipher(PublicKey publicKey) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        return cipher;
    }

    public byte[] encrypt(byte[] clearMessage) throws Exception {
        return _cipher.doFinal(clearMessage);
    }
}


//
// Decrypts byte arrays using an asymmetric key pair.
//
class AsymmetricDecryptor {
    //TODO: Put padding back
    public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";
    // public static final String CIPHER_ALGORITHM = "RSA/ECB/NoPadding";

    private Cipher _cipher;

    public AsymmetricDecryptor(PrivateKey privateKey) throws Exception {
        _cipher = newCipher(privateKey);
    }

    public static Cipher newCipher(PrivateKey privateKey) throws Exception {
        Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return cipher;
    }

    public byte[] decrypt(byte[] encryptedMessage) throws Exception {
        return _cipher.doFinal(encryptedMessage);
    }
}


//
// Reads public and private keys from a file.
//

class AsymmetricKeyReader {
    public static final String KEY_ALGORITHM = "RSA";

    final private static Logger logger = LoggerFactory.getLogger(AsymmetricKeyReader.class);

    /*public static PrivateKey readPrivateKey(String filenameDer) throws Exception
    {
        byte[] keyBytes = readAllBytes(filenameDer);
      logger.debug("Key = " +  Hex.encodeHexString(keyBytes));
      return readPrivateKey(keyBytes);
    }*/
    public static PrivateKey readPrivateKey(byte[] keyBytes) throws Exception {

        KeyFactory keyFactory = newKeyFactory();
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        return keyFactory.generatePrivate(spec);
    }

    /*
      public static PublicKey readPublicKey(String filenameDer) throws Exception
      {
        byte[] keyBytes = readAllBytes(filenameDer);
        return readPublicKey(keyBytes);
      }*/
    public static PublicKey readPublicKey(byte[] keyBytes) throws Exception {
        KeyFactory keyFactory = newKeyFactory();
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        return keyFactory.generatePublic(spec);

    }

    private static byte[] readAllBytes(String filename) throws Exception {
        return Files.readAllBytes(Paths.get(filename));
    }

    private static KeyFactory newKeyFactory() throws Exception {
        return KeyFactory.getInstance(KEY_ALGORITHM);
    }
}
