package com.robustdb.server.util;


import com.robustdb.server.exception.RobustDBValidationException;

public final class Requires {


    public static <T> T requireNonNull(T obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        return obj;
    }


    public static <T> T requireNonNull(T obj, String message) {
        if (obj == null) {
            throw new NullPointerException(message);
        }
        return obj;
    }


    public static void requireTrue(boolean expression) {
        if (!expression) {
            throw new RobustDBValidationException();
        }
    }


    public static void requireTrue(boolean expression, Object message) {
        if (!expression) {
            throw new RobustDBValidationException(String.valueOf(message));
        }
    }


    public static void requireTrue(boolean expression, String fmt, Object... args) {
        if (!expression) {
            throw new RobustDBValidationException(String.format(fmt, args));
        }
    }

    private Requires() {
    }
}
