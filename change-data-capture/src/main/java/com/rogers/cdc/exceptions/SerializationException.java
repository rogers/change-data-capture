package com.rogers.cdc.exceptions;




public class SerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String msg) {
        super(msg);
    }

    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

}