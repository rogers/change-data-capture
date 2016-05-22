package com.rogers.cdc.exceptions;

public class InvalidTypeException extends RuntimeException {


    public InvalidTypeException(String msg) {
        super(msg);
    }

    public InvalidTypeException(String msg, Throwable cause) {
        super(msg, cause);
    }

}