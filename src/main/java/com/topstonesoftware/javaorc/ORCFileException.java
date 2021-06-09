package com.topstonesoftware.javaorc;

/**
 * A custom exception for writing and reading ORC files. This exception wraps other exceptions, like IOException so
 * that an application does not have to handle more than one exception.
 */
public class ORCFileException extends Exception {

    public ORCFileException(String message) {
        super(message);
    }

    public ORCFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
