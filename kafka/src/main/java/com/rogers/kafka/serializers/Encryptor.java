package com.rogers.kafka.serializers;



//import  com.goldengate.delivery.handler.kafka.util.EncryptedMessage;
import  com.rogers.kafka.crypto.key.*;

import java.nio.file.*;
import java.security.*;
import java.security.spec.*;

import javax.crypto.*;
import javax.crypto.spec.*;

import java.util.Random;



//import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Encryptor {
	 
	  protected static final String SECRET_KEY_ALGORITHM = "AES";
	  public static final String ENCRYPTOR_CONFIG = "crypto.encryptor";

	  
	  protected KeyProvider provider;
	  final private static Logger logger = LoggerFactory.getLogger(Encryptor.class);
	  
	  /*
	  public static void main(String [ ] args)
		{
          try { 
        	KeyProvider provider = new TestKeyProvider();
        	Encryptor encryptor = new Encryptor(provider);
		    String input = "My test: This is a test string to make sure the encyption works. Need to make it longer. Very long. Just to make sure!";
		    EncryptedMessage msg = encryptor.encrypt(input);
		    byte[] enc_bytes = msg.toByteArray();
		    EncryptedMessage msg2   = new EncryptedMessage(enc_bytes);
		    String output =  encryptor.DecryptToString(msg2);
		    assert input == output : "Input != Output";
       } catch (Exception e1) {		
    	   System.out.println("Caught expecption:" + e1);
		}
		  
			
		}
		*/
	 
	  
	  public Encryptor(KeyProvider _provider){
		  provider = _provider; 
		  
	  }
	  public  byte[]  encrypt(String message,  String[] public_keys) throws Exception {
		 return encrypt(message.getBytes(), public_keys);
	  }
	  public  byte[]  encrypt(String message,  String public_key) throws Exception {
			 return encrypt(message.getBytes(), public_key);
		  }
      abstract  public  byte[] encrypt(byte[] messageBytes, String[] public_keys) throws Exception;
      
      public  byte[] encrypt(byte[] messageBytes, String public_key) throws Exception{
    	  String[] arr = {public_key};
    	  return encrypt(messageBytes, arr);
      }
      abstract public  byte[] decrypt(byte[] payload)  throws Exception;
      public  String decryptToString(byte[] payload)  throws Exception {
    	  byte[] receivedMessageBytes = decrypt(payload);
    	  String receivedMessage = new String(receivedMessageBytes, "UTF8");
         //System.out.printf("output message: %s\n", receivedMessage);
          return receivedMessage;
      }
      protected byte[] decryptImp(byte[] encryptedMessage, byte[] encryptedKey, byte[] encryptedIV, String keyName)  throws Exception{

    	  PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(provider.getKeyVersion(keyName).getMaterial());
    	 // try {
    	  AsymmetricDecryptor asymmetricDecryptor = new AsymmetricDecryptor(privateKey);
    	  //byte[] receivedSecretKeyBytes ="d".getBytes();
    	  
    	  byte[]    receivedSecretKeyBytes = asymmetricDecryptor.decrypt(encryptedKey);
         
    	 // logger.info("receivedSecretKeyBytes:" + Hex.encodeHexString(encryptedKey ));
    	  //logger.info("Decrypted SecretKey:" + Hex.encodeHexString(receivedSecretKeyBytes));
		  
          SecretKey receivedSecretKey = new SecretKeySpec(receivedSecretKeyBytes, SECRET_KEY_ALGORITHM);
         // assert receivedSecretKey.getEncoded().length == SECRET_KEY_LENGTH_BITS: "Secret key is " + receivedSecretKey.getEncoded().length + " long, expecting " + SECRET_KEY_LENGTH_BITS;
          // Decrypt the symmetric key initialization vector with the private key.
          byte[] receivedIVBytes = asymmetricDecryptor.decrypt(encryptedIV);
          IvParameterSpec receivedIV = new IvParameterSpec(receivedIVBytes);

          // Decrypt the message.
          SymmetricDecryptor symmetricDecryptor = new SymmetricDecryptor(receivedSecretKey, receivedIV);
          byte[] receivedMessageBytes = symmetricDecryptor.decrypt(encryptedMessage);

          // The message that was received.
         // System.out.printf("output message: %s\n", receivedMessage);
          return receivedMessageBytes; 
    	 // } catch (Exception e1) {		
     	   //     System.out.println("Error decrypting expecption:" + e1);
  		 // }
      }
      

	  
}


class SymmetricEncryptor
{
  
 static final String KEY_ALGORITHM    = "AES";
  public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";
  //public static final int    KEY_LENGTH_BITS  = 256;
  //TODO Install Java Cryptography Extension to allow 256 bits
  public static final int    KEY_LENGTH_BITS  = 128; 
  public static final int    IV_LENGTH_BYTES  = 16;                           // 256/8 = 32; however, iv must be 16 bytes long (TODO: why?).

  
  final private static Logger logger = LoggerFactory.getLogger(SymmetricEncryptor.class);
  
  private SecretKey _key;
  private IvParameterSpec _iv;
  private Cipher _cipher;

  public SymmetricEncryptor() throws Exception
  {
    _key = generateSymmetricKey();

    _iv = generateInitializationVector();
    _cipher = newCipher(_key, _iv);
  }
  
  public byte[] encrypt(byte[] clearMessage) throws Exception
  {
    return _cipher.doFinal(clearMessage);
  }

  public SecretKey getKey()
  {
    return _key;
  }

  public IvParameterSpec getInitializationVector()
  {
    return _iv;
  }

  private static SecretKey generateSymmetricKey() throws Exception
  {
    KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGORITHM);
    SecureRandom random = new SecureRandom();
    generator.init(KEY_LENGTH_BITS, random);
    logger.debug("Generate Key");
    return generator.generateKey();
    //return new SecretKeySpec(Hex.decodeHex("cb024600dce7148b8ddc5d6c111fbd85".toCharArray()),  KEY_ALGORITHM);

  }

  private static IvParameterSpec generateInitializationVector()
  {
    SecureRandom random = new SecureRandom();

    return new IvParameterSpec("beb6f54c39089a9d".getBytes());
  }

  private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception
  {
	
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, symmetricKey, iv);
    return cipher;
  }
}


//
// Decrypts byte arrays using a symmetric key.
//
class SymmetricDecryptor
{
  public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

  private Cipher _cipher;

  public SymmetricDecryptor(SecretKey key, IvParameterSpec iv) throws Exception
  {
    _cipher = newCipher(key, iv);
  }

  public byte[] decrypt(byte[] encryptedMessage) throws Exception
  {
    return _cipher.doFinal(encryptedMessage);
  }

  private static Cipher newCipher(SecretKey symmetricKey, IvParameterSpec iv) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, symmetricKey, iv);
    return cipher;
  }
}


//
// Encrypts byte arrays using an asymmetric key pair.
//
class AsymmetricEncryptor
{
	//TODO: Put padding back
  public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";
  //public static final String CIPHER_ALGORITHM = "RSA/ECB/NoPadding";

  
  private Cipher _cipher;

  public AsymmetricEncryptor(PublicKey publicKey) throws Exception
  {
    _cipher = newCipher(publicKey);
  }

  public byte[] encrypt(byte[] clearMessage) throws Exception
  {
    return _cipher.doFinal(clearMessage);
  }

  private static Cipher newCipher(PublicKey publicKey) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
    return cipher;    
  }
}


//
// Decrypts byte arrays using an asymmetric key pair.
//
class AsymmetricDecryptor
{
	//TODO: Put padding back
  public static final String CIPHER_ALGORITHM = "RSA/ECB/PKCS1Padding";
 // public static final String CIPHER_ALGORITHM = "RSA/ECB/NoPadding";

  private Cipher _cipher;

  public AsymmetricDecryptor(PrivateKey privateKey) throws Exception
  {
    _cipher = newCipher(privateKey);
  }

  public byte[] decrypt(byte[] encryptedMessage) throws Exception
  {
    return _cipher.doFinal(encryptedMessage);
  }

  public static Cipher newCipher(PrivateKey privateKey) throws Exception
  {
    Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, privateKey);
    return cipher;    
  }
}


//
// Reads public and private keys from a file.
//

class AsymmetricKeyReader
{
  public static final String KEY_ALGORITHM = "RSA";
  
  final private static Logger logger = LoggerFactory.getLogger(AsymmetricKeyReader.class);

  /*public static PrivateKey readPrivateKey(String filenameDer) throws Exception
  {
      byte[] keyBytes = readAllBytes(filenameDer);
    logger.debug("Key = " +  Hex.encodeHexString(keyBytes));
    return readPrivateKey(keyBytes);
  }*/
  public static PrivateKey readPrivateKey(byte [] keyBytes) throws Exception
  {
   
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
  public static PublicKey readPublicKey(byte[] keyBytes) throws Exception{
	  KeyFactory keyFactory = newKeyFactory();
	  X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
	  return keyFactory.generatePublic(spec);
	  
  }

  private static byte[] readAllBytes(String filename) throws Exception
  {
    return Files.readAllBytes(Paths.get(filename));
  }

  private static KeyFactory newKeyFactory() throws Exception
  {
    return KeyFactory.getInstance(KEY_ALGORITHM);
  }
}
