package com.rogers.cdc.exceptions;



public class ConfigException extends RuntimeException {


    public ConfigException(String msg) {
        super(msg);
    }

    public ConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }

}