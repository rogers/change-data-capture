package com.rogers.cdc.api.mutations;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public enum CloumnType {
	 INT       (  Integer.class),
	 BIGINT    (Long.class),
     DOUBLE    (  Double.class),
     FLOAT     (  Float.class),
     BOOLEAN   (Boolean.class),
	 STRING     (String.class),
     BYTES      (ByteBuffer.class),
     ARRAY      ( List.class),
     MAP       ( Map.class),
     DECIMAL   (  BigDecimal.class),
     DATE      ( Date.class),
     TIME      ( Date.class),
     TIMESTAMP ( Date.class);
    
	 final Class<?> javaType;
	 private CloumnType( Class<?> javaType) {
         this.javaType = javaType;
     }
}
