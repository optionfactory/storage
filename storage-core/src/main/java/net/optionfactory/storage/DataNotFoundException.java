package net.optionfactory.storage;

/**
 * Exception thrown when a requested file or resource is not found in the storage.
 */
public class DataNotFoundException extends RuntimeException {
    /**
     * Constructs a new DataNotFoundException with no detail message.
     */
    public DataNotFoundException() {
    }

    /**
     * Constructs a new DataNotFoundException with the specified detail message.
     *
     * @param message the detail message
     */
    public DataNotFoundException(String message) {
        super(message);
    }

    /**
     * Constructs a new DataNotFoundException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public DataNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
