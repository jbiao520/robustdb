package com.robustdb.server.exception;

public class RobustDBValidationException extends RuntimeException{
    public RobustDBValidationException() {
    }

    public RobustDBValidationException(String message) {
        super(message);
    }
}
