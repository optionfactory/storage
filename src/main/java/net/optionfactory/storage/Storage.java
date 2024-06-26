package net.optionfactory.storage;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

public interface Storage {

    void store(String name, Path sourceFile, Permissions permissions);

    void store(Path target, Path sourceFile, Permissions permissions);

    void store(Path target, InputStream in, Permissions permissions);

    /**
     * Avoid to use! Save the RAM save the world!
     */
    @Deprecated
    void store(String name, byte[] data, String mimeType, Permissions permissions);

    InputStream retrieve(Path path);

    List<Path> list();

    List<Path> list(Path path);

    void copy(String sourceName, String targetName);

    String absoluteUrl(String... paths);

    void delete(Path target);

    void publish(String name);

    void unpublish(String name);
}
