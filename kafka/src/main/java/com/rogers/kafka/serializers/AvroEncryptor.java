package com.rogers.kafka.serializers;

import  com.rogers.kafka.crypto.key.*;
import  com.rogers.kafka.crypto.key.KeyProvider.KeyVersion;


import crypto.avro.AvroEncryptedMessage;
import crypto.avro.AvroKeyVersion;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.List;
import java.util.ArrayList;

import java.util.Random;

import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroEncryptor extends Encryptor {
	  protected static final byte MAGIC_BYTE = 0x0;
	  protected static final short SCHEME_ID =1;
	  protected static final int idSize = 4;


	  private final DecoderFactory decoderFactory = DecoderFactory.get();
	  private final EncoderFactory encoderFactory = EncoderFactory.get();
	  DatumWriter<AvroEncryptedMessage> writer;
	  DatumReader<AvroEncryptedMessage> reader;
	  
	  final private static Logger logger = LoggerFactory.getLogger(Encryptor.class);
	  
	  
	  public AvroEncryptor(KeyProvider _provider){
		  super(_provider); 
		  try{ 
	         writer = new SpecificDatumWriter<AvroEncryptedMessage>(AvroEncryptedMessage.class);
	         reader = new SpecificDatumReader<AvroEncryptedMessage>(AvroEncryptedMessage.class);
		  } catch (RuntimeException e) {
  		      throw new SerializationException("Failed to create Avro reader and writer  ", e);
  		}

		  
	  }
	  @Override
      public  byte[] encrypt(byte[] messageBytes, String[] public_keys) throws Exception{
    	// Encrypt the message with a new symmetric key.
		  AvroEncryptedMessage msg =  new AvroEncryptedMessage();
          SymmetricEncryptor symmetricEncryptor = new SymmetricEncryptor();
          byte[] encryptedMessage = symmetricEncryptor.encrypt(messageBytes);
          msg.setEncryptedMessage(ByteBuffer.wrap(encryptedMessage));
        
 	     
 	      List<AvroKeyVersion> keys = new  ArrayList<AvroKeyVersion> ();  
          for (String public_key :public_keys ){ 
        	 KeyVersion currentKey = provider.getCurrentPublicKey(public_key);
        	 String keyVersionName = currentKey.getVersionName();
             PublicKey publicKey = AsymmetricKeyReader.readPublicKey(currentKey.getMaterial());

            // Encrypt the symmetric key with the public key.
            AsymmetricEncryptor asymmetricEncriptor = new AsymmetricEncryptor(publicKey);
            byte[] secretKeyBytes = symmetricEncryptor.getKey().getEncoded();
            byte[] encryptedSecretKey = asymmetricEncriptor.encrypt(secretKeyBytes);

            logger.debug("Key = {}", Hex.encodeHexString(secretKeyBytes));
            logger.debug("encrypted Secret Key {}", Hex.encodeHexString(encryptedSecretKey));

            // Encrypt the symmetric key initialization vector with the public key.
            byte[] ivBytes = symmetricEncryptor.getInitializationVector().getIV();
            byte[] encryptedIV = asymmetricEncriptor.encrypt(ivBytes);
            logger.debug("IV = {}",  Hex.encodeHexString(ivBytes));
            keys.add(new AvroKeyVersion(ByteBuffer.wrap(encryptedSecretKey), ByteBuffer.wrap(encryptedIV), keyVersionName));

          }
          msg.setKeys(keys);
          return  serialize(msg);

	  }
	  @Override
      public  byte[] decrypt(byte[] payload)  throws Exception {
		  AvroEncryptedMessage msg = deserialize(payload);
		  AvroKeyVersion key = findKeyVersion(msg.getKeys());
   	      return decryptImp(msg.encryptedMessage.array(), key.getEncryptedKey().array(),  key.encryptedIV.array(), key.getVersionName().toString());
      }
      private AvroKeyVersion findKeyVersion(List<AvroKeyVersion> keys) throws IOException{
    	  for(AvroKeyVersion key: keys){
    		  if(provider.getKeyVersion(key.getVersionName().toString()) != null){
    			  
    			  return key; 
    		  }
    	  }
    	  throw new IOException("Could not find private key version:"); 
    	  
      }
      private ByteBuffer getByteBuffer(byte[] payload) {
    	    ByteBuffer buffer = ByteBuffer.wrap(payload);
    	    if (buffer.get() != MAGIC_BYTE) {
    	      throw new SerializationException("Unknown magic byte!");
    	    }
    	    return buffer;
    	  }
      private AvroEncryptedMessage deserialize(byte[] payload){
    	  int id = -1;
  		try {
  			ByteBuffer buffer = getByteBuffer(payload);
  			id = buffer.getInt();
  			// TODO: Right now we have only one schema... add suport for picking the right scema based on the ID
  			if (id != SCHEME_ID) {
      	      throw new SerializationException("Unknown schema ");
      	    }
  			int start = buffer.position() + buffer.arrayOffset();
  			int length = buffer.limit() - 1 - idSize;
  			AvroEncryptedMessage msg =
  			          reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));//TODO might want to user the reuser option
  		    return msg;
  		} catch (IOException e) {
  		      throw new SerializationException("Error deserializing Avro SecureMessage  ", e);
  		} catch (RuntimeException e) {
  		      throw new SerializationException("Error deserializing Avro SecureMessage  ", e);
  		}
  		
  	}
      private byte[] serialize(AvroEncryptedMessage msg){
    	  try {
    	     ByteArrayOutputStream out = new ByteArrayOutputStream();
    	     BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
    	     out.write(MAGIC_BYTE);
    	     out.write(ByteBuffer.allocate(idSize).putInt(SCHEME_ID).array());
    	     writer.write(msg, encoder);
             encoder.flush();
             byte[] bytes = out.toByteArray();
             out.close();
             return bytes;
    	  } catch (IOException e) {
    	      throw new SerializationException("Error serializing Avro message", e);
    	 }
  	}  
}



