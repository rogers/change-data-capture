package com.rogers.kafka.serializers;




//import com.goldengate.delivery.handler.kafka.KafkaHandler;
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

public class BinaryEncryptor extends Encryptor {

	  public static final int SECRET_KEY_LENGTH_BITS = 1024;
	
	  final private static Logger logger = LoggerFactory.getLogger(BinaryEncryptor.class);
	  
	  
	  public BinaryEncryptor(KeyProvider _provider){
		  super(_provider);	  
	  }

	  @Override
      public byte[] encrypt(byte[] messageBytes, String[] public_keys) throws Exception{
		  //TODO: add support for multiple keys
		  assert(public_keys.length == 1);
		  String public_key = public_keys[0];
    	// Encrypt the message with a new symmetric key.
          SymmetricEncryptor symmetricEncryptor = new SymmetricEncryptor();
          byte[] encryptedMessage = symmetricEncryptor.encrypt(messageBytes);
          
          PublicKey publicKey = AsymmetricKeyReader.readPublicKey(provider.getCurrentPublicKey(public_key).getMaterial());
          // Encrypt the symmetric key with the public key.
          AsymmetricEncryptor asymmetricEncriptor = new AsymmetricEncryptor(publicKey);
          byte[] secretKeyBytes = symmetricEncryptor.getKey().getEncoded();
          byte[] encryptedSecretKey = asymmetricEncriptor.encrypt(secretKeyBytes);

         /* logger.debug("Run  = " + new Random().nextInt(100));
          logger.debug("Key = " + Hex.encodeHexString(secretKeyBytes));
          logger.debug("encrypted Secret Key = " + Hex.encodeHexString(encryptedSecretKey));
*/
          // Encrypt the symmetric key initialization vector with the public key.
          byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
          byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);
        //  logger.debug("IV = " + Hex.encodeHexString(ivBytes));
          
          
          EncryptedMessage msg = new EncryptedMessage(encryptedMessage, encryptedSecretKey, encryptedIV, public_key);
 
          return msg.toByteArray();

	  }
      @Override
      public  byte[] decrypt(byte[] payload)  throws Exception {
    	  EncryptedMessage msg = new EncryptedMessage(payload);
    	  //System.out.print("Dec Message = " + new String(msg.getMessage()) + "\n");
    	   return decryptImp(msg.getMessage(), msg.getKey(),  msg.getIV(), msg.getKeyName());
    	// Read private key from file.
    	  /*
         // PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(PRIVATE_KEY_FILENAME);
          PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(provider.getCurrentKey("private").getMaterial());
          //PrivateKey privateKey = AsymmetricKeyReader.readPrivateKey(Hex.decodeHex(PRIVATE_KEY_HEX_STRING.toCharArray()));
    	  AsymmetricDecryptor asymmetricDecryptor = new AsymmetricDecryptor(privateKey);
    	  byte[] receivedSecretKeyBytes ="d".getBytes();
    	  try {
               receivedSecretKeyBytes = asymmetricDecryptor.decrypt(msg.getKey());
          } catch (Exception e1) {		
   	        System.out.println("Error decrypting expecption:" + e1);
		  }
    	  logger.debug("receivedSecretKeyBytes:" + Hex.encodeHexString(msg.getKey()));
    	  logger.debug("Decrypted SecretKey:" + Hex.encodeHexString(receivedSecretKeyBytes));
		  
          SecretKey receivedSecretKey = new SecretKeySpec(receivedSecretKeyBytes, SECRET_KEY_ALGORITHM);
          assert receivedSecretKey.getEncoded().length == SECRET_KEY_LENGTH_BITS: "Secret key is " + receivedSecretKey.getEncoded().length + " long, expecting " + SECRET_KEY_LENGTH_BITS;
          // Decrypt the symmetric key initialization vector with the private key.
          byte[] receivedIVBytes = asymmetricDecryptor.decrypt(msg.getIV());
          IvParameterSpec receivedIV = new IvParameterSpec(receivedIVBytes);

          // Decrypt the message.
          SymmetricDecryptor symmetricDecryptor = new SymmetricDecryptor(receivedSecretKey, receivedIV);
          byte[] receivedMessageBytes = symmetricDecryptor.decrypt(msg.getMessage() );

          // The message that was received.
         // System.out.printf("output message: %s\n", receivedMessage);
          return receivedMessageBytes; */
    	  
      }  
}