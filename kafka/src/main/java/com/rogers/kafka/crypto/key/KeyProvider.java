package com.rogers.kafka.crypto.key;


import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
//import com.google.gson.stream.JsonReader;
//import com.google.gson.stream.JsonWriter;
//import org.apache.commons.io.Charsets;
//import org.apache.commons.io.
import javax.crypto.KeyGenerator;

/**
 * A very naive implementation  of a secret key provider. Provides an
 * abstraction to separate key storage from users of encryption. 
 * A lot of code copied from: https://github.com/apache/hadoop/blob/c1d50a91f7c05e4aaf4655380c8dcd11703ff158/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/crypto/key/KeyProvider.java
 * <P/>
 * <code>KeyProvider</code> 
 */
public abstract class KeyProvider {
	public static final String PROVIDER_CONFIG = "crypto.key_provider";
	
	  /**
	   * The combination of both the key version name and the key material.
	   */
	  public static class KeyVersion {
	    private final String name;
	    private final String versionName;
	    private final byte[] material;

	    protected KeyVersion(String name, String versionName,
	                         byte[] material) {
	      this.name = name;
	      this.versionName = versionName;
	      this.material = material;
	    }
	    
	    public String getName() {
	      return name;
	    }

	    public String getVersionName() {
	      return versionName;
	    }

	    public byte[] getMaterial() {
	      return material;
	    }

	    public String toString() {
	      StringBuilder buf = new StringBuilder();
	      buf.append("key(");
	      buf.append(versionName);
	      buf.append(")=");
	      if (material == null) {
	        buf.append("null");
	      } else {
	        for(byte b: material) {
	          buf.append(' ');
	          int right = b & 0xff;
	          if (right < 0x10) {
	            buf.append('0');
	          }
	          buf.append(Integer.toHexString(right));
	        }
	      }
	      return buf.toString();
	    }
	  }
	  /**
	   * Key metadata that is associated with the key.
	   */
	  public static class Metadata {
	    private final static String CIPHER_FIELD = "cipher";
	    private final static String BIT_LENGTH_FIELD = "bitLength";
	    private final static String CREATED_FIELD = "created";
	    private final static String DESCRIPTION_FIELD = "description";
	    private final static String VERSIONS_FIELD = "versions";
	    private final static String ATTRIBUTES_FIELD = "attributes";

	    private final String cipher;
	    private final int bitLength;
	    private final String description;
	    private final Date created;
	    private int versions;
	    private Map<String, String> attributes;

	    protected Metadata(String cipher, int bitLength, String description,
	        Map<String, String> attributes, Date created, int versions) {
	      this.cipher = cipher;
	      this.bitLength = bitLength;
	      this.description = description;
	      this.attributes = (attributes == null || attributes.isEmpty())
	                        ? null : attributes;
	      this.created = created;
	      this.versions = versions;
	    }

	    public String toString() {
	      final StringBuilder metaSB = new StringBuilder();
	      metaSB.append("cipher: ").append(cipher).append(", ");
	      metaSB.append("length: ").append(bitLength).append(", ");
	      metaSB.append("description: ").append(description).append(", ");
	      metaSB.append("created: ").append(created).append(", ");
	      metaSB.append("version: ").append(versions).append(", ");
	      metaSB.append("attributes: ");
	      if ((attributes != null) && !attributes.isEmpty()) {
	        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
	          metaSB.append("[");
	          metaSB.append(attribute.getKey());
	          metaSB.append("=");
	          metaSB.append(attribute.getValue());
	          metaSB.append("], ");
	        }
	        metaSB.deleteCharAt(metaSB.length() - 2);  // remove last ', '
	      } else {
	        metaSB.append("null");
	      }
	      return metaSB.toString();
	    }

	    public String getDescription() {
	      return description;
	    }

	    public Date getCreated() {
	      return created;
	    }

	    public String getCipher() {
	      return cipher;
	    }

	    @SuppressWarnings("unchecked")
	    public Map<String, String> getAttributes() {
	      return (attributes == null) ? Collections.EMPTY_MAP : attributes;
	    }

	    /**
	     * Get the algorithm from the cipher.
	     * @return the algorithm name
	     */
	    public String getAlgorithm() {
	      int slash = cipher.indexOf('/');
	      if (slash == - 1) {
	        return cipher;
	      } else {
	        return cipher.substring(0, slash);
	      }
	    }

	    public int getBitLength() {
	      return bitLength;
	    }

	    public int getVersions() {
	      return versions;
	    }

	    protected int addVersion() {
	      return versions++;
	    }

	    /**
	     * Serialize the metadata to a set of bytes.
	     * @return the serialized bytes
	     * @throws IOException
	     */
	    /*
	    protected byte[] serialize() throws IOException {
	      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	      JsonWriter writer = new JsonWriter(
	          new OutputStreamWriter(buffer, Charsets.UTF_8));
	      try {
	        writer.beginObject();
	        if (cipher != null) {
	          writer.name(CIPHER_FIELD).value(cipher);
	        }
	        if (bitLength != 0) {
	          writer.name(BIT_LENGTH_FIELD).value(bitLength);
	        }
	        if (created != null) {
	          writer.name(CREATED_FIELD).value(created.getTime());
	        }
	        if (description != null) {
	          writer.name(DESCRIPTION_FIELD).value(description);
	        }
	        if (attributes != null && attributes.size() > 0) {
	          writer.name(ATTRIBUTES_FIELD).beginObject();
	          for (Map.Entry<String, String> attribute : attributes.entrySet()) {
	            writer.name(attribute.getKey()).value(attribute.getValue());
	          }
	          writer.endObject();
	        }
	        writer.name(VERSIONS_FIELD).value(versions);
	        writer.endObject();
	        writer.flush();
	      } finally {
	        writer.close();
	      }
	      return buffer.toByteArray();
	    }
*/
	    /**
	     * Deserialize a new metadata object from a set of bytes.
	     * @param bytes the serialized metadata
	     * @throws IOException
	     */
	    /*
	    protected Metadata(byte[] bytes) throws IOException {
	      String cipher = null;
	      int bitLength = 0;
	      Date created = null;
	      int versions = 0;
	      String description = null;
	      Map<String, String> attributes = null;
	      JsonReader reader = new JsonReader(new InputStreamReader
	        (new ByteArrayInputStream(bytes), Charsets.UTF_8));
	      try {
	        reader.beginObject();
	        while (reader.hasNext()) {
	          String field = reader.nextName();
	          if (CIPHER_FIELD.equals(field)) {
	            cipher = reader.nextString();
	          } else if (BIT_LENGTH_FIELD.equals(field)) {
	            bitLength = reader.nextInt();
	          } else if (CREATED_FIELD.equals(field)) {
	            created = new Date(reader.nextLong());
	          } else if (VERSIONS_FIELD.equals(field)) {
	            versions = reader.nextInt();
	          } else if (DESCRIPTION_FIELD.equals(field)) {
	            description = reader.nextString();
	          } else if (ATTRIBUTES_FIELD.equalsIgnoreCase(field)) {
	            reader.beginObject();
	            attributes = new HashMap<String, String>();
	            while (reader.hasNext()) {
	              attributes.put(reader.nextName(), reader.nextString());
	            }
	            reader.endObject();
	          }
	        }
	        reader.endObject();
	      } finally {
	        reader.close();
	      }
	      this.cipher = cipher;
	      this.bitLength = bitLength;
	      this.created = created;
	      this.description = description;
	      this.attributes = attributes;
	      this.versions = versions;
	    }*/
	  }
	 /**
	   * Constructor.
	   * 
	   * @param conf configuration for the provider
	   */
	  //TODO:  Map<?,?> is messy. Validate configs, or create a special  Config class
	  public KeyProvider(Map<?, ?> configs) {

	  }
	  /**
	   * Get the key material for a specific version of the key. This method is used
	   * when decrypting data.
	   * @param versionName the name of a specific version of the key
	   * @return the key material
	   * @throws IOException
	   */
	  public abstract KeyVersion getKeyVersion(String versionName
	                                            ) throws IOException;

	  /**
	   * Get the key names for all keys.
	   * @return the list of key names
	   * @throws IOException
	   */
	 // public abstract List<String> getKeys() throws IOException;
	  
	  
	  /**
	   * Get the current version of the key, which should be used for encrypting new
	   * data.
	   * @param name the base name of the key
	   * @return the version name of the current version of the key or null if the
	   *    key version doesn't exist
	   * @throws IOException
	   */
	  public abstract  KeyVersion getCurrentPublicKey(String name) throws IOException ;
	  /**
	   * Get all the public keys the producer should use to encrypt the message
	   * @return a list of all public keys
	   * @throws IOException
	   */
	  public abstract  String[] getAllPublicKeys() throws IOException ;
	 /* public KeyVersion getCurrentKey(String name) throws IOException {
	    Metadata meta = getMetadata(name);
	    if (meta == null) {
	      return null;
	    }
	    return getKeyVersion(buildVersionName(name, meta.getVersions() - 1));
	  }*/
	  /**
	   * Get metadata about the key.
	   * @param name the basename of the key
	   * @return the key's metadata or null if the key doesn't exist
	   * @throws IOException
	   */
	  /*public abstract Metadata getMetadata(String name) throws IOException;*/

	  
	  public abstract List<KeyVersion> getKeyVersions(String name) throws IOException;
	  
	  /**
	   * Can be used by implementing classes to close any resources
	   * that require closing
	   */
	  public void close() throws IOException {
	    // NOP
	  }
	  /**
	   * Get the algorithm from the cipher.
	   *
	   * @return the algorithm name
	   */
	  private String getAlgorithm(String cipher) {
	    int slash = cipher.indexOf('/');
	    if (slash == -1) {
	      return cipher;
	    } else {
	      return cipher.substring(0, slash);
	    }
	  }
	  /**
	   * Generates a key material.
	   *
	   * @param size length of the key.
	   * @param algorithm algorithm to use for generating the key.
	   * @return the generated key.
	   * @throws NoSuchAlgorithmException
	   */
	  protected byte[] generateKey(int size, String algorithm)
	      throws NoSuchAlgorithmException {
	    algorithm = getAlgorithm(algorithm);
	    KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm);
	    keyGenerator.init(size);
	    byte[] key = keyGenerator.generateKey().getEncoded();
	    return key;
	  }
	  /**
	   * Roll a new version of the given key.
	   * @param name the basename of the key
	   * @param material the new key material
	   * @return the name of the new version of the key
	   * @throws IOException
	   */
	  /*public abstract KeyVersion rollNewVersion(String name,
	                                             byte[] material
	                                            ) throws IOException;*/
	  /**
	   * Roll a new version of the given key generating the material for it.
	   * <p/>
	   * This implementation generates the key material and calls the
	   * {@link #rollNewVersion(String, byte[])} method.
	   *
	   * @param name the basename of the key
	   * @return the name of the new version of the key
	   * @throws IOException
	   */
	  //TODO
	 /* public KeyVersion rollNewVersion(String name) throws NoSuchAlgorithmException,
	                                                       IOException {
	    Metadata meta = getMetadata(name);
	    byte[] material = generateKey(meta.getBitLength(), meta.getCipher());
	    return rollNewVersion(name, material);
	  }*/

	 

	  /**
	   * Split the versionName in to a base name. Converts "/aaa/bbb/3" to
	   * "/aaa/bbb".
	   * @param versionName the version name to split
	   * @return the base name of the key
	   * @throws IOException
	   */
	  public static String getBaseName(String versionName) throws IOException {
	    int div = versionName.lastIndexOf('@');
	    if (div == -1) {
	       return versionName;
	    }
	    return versionName.substring(0, div);
	  }

	  /**
	   * Build a version string from a basename and version number. Converts
	   * "/aaa/bbb" and 3 to "/aaa/bbb@3".
	   * @param name the basename of the key
	   * @param version the version of the key
	   * @return the versionName of the key.
	   */
	  protected static String buildVersionName(String name, int version) {
		if (version >=0){
	       return name + "@" + version;
		}else{
			return name;
		}
	  }
	  
	  
	  /**
	   * Ensures that any changes to the keys are written to persistent store.
	   * @throws IOException
	   */
	  public abstract void flush() throws IOException;
	  
	  //TODO: Create operations?

}
