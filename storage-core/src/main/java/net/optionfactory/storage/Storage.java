package net.optionfactory.storage;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for storing, retrieving, and managing files across different storage backends.
 */
public interface Storage {

    /**
     * Stores a file from a local path using a specific name.
     *
     * @param name        the name of the file in the storage
     * @param sourceFile  the local path to the source file
     * @param permissions the permissions to apply to the stored file
     */
    void store(String name, Path sourceFile, Permissions permissions);

    /**
     * Stores a file from a local path to a target path in the storage.
     *
     * @param target      the target path in the storage
     * @param sourceFile  the local path to the source file
     * @param permissions the permissions to apply to the stored file
     */
    void store(Path target, Path sourceFile, Permissions permissions);

    /**
     * Stores a file from an input stream to a target path in the storage.
     *
     * @param target      the target path in the storage
     * @param in          the input stream providing the file content
     * @param permissions the permissions to apply to the stored file
     */
    void store(Path target, InputStream in, Permissions permissions);

    /**
     * Avoid to use! Save the RAM save the world!
     *
     * @param name        the name of the file in the storage
     * @param data        the byte array containing the file data
     * @param mimeType    the MIME type of the file
     * @param permissions the permissions to apply to the stored file
     */
    @Deprecated
    void store(String name, byte[] data, String mimeType, Permissions permissions);

    /**
     * Retrieves a file as an input stream from the specified path.
     *
     * @param path the path to the file in the storage
     * @return an input stream for reading the file content
     * @throws DataNotFoundException if the file is not found
     */
    InputStream retrieve(Path path);

    /**
     * Caches an input stream to a local temporary file.
     *
     * @param inputStream the input stream to cache
     * @return the path to the local temporary file
     */
    Path cacheLocally(InputStream inputStream);

    /**
     * Lists all files in the storage root.
     *
     * @return a list of paths representing the files in the storage
     */
    List<Path> list();

    /**
     * Lists all files in the specified path within the storage.
     *
     * @param path the path to list files from
     * @return a list of paths representing the files in the specified storage path
     */
    List<Path> list(Path path);

    /**
     * Copies a file from a source path to a target path within the storage.
     *
     * @param sourceName the source path in the storage
     * @param targetName the target path in the storage
     */
    void copy(Path sourceName, Path targetName);

    /**
     * Resolves a relative path to an absolute URL.
     *
     * @param paths the path components to combine
     * @return the absolute URL as a string
     */
    String absoluteUrl(String... paths);

    /**
     * Deletes a file or directory from the storage.
     *
     * @param target the path to the file or directory to delete
     */
    void delete(Path target);

    /**
     * Makes a file publicly accessible.
     *
     * @param name the name or path of the file to publish
     */
    void publish(String name);

    /**
     * Revokes public access to a file.
     *
     * @param name the name or path of the file to unpublish
     */
    void unpublish(String name);
}
