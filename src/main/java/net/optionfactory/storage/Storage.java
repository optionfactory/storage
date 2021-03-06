package net.optionfactory.storage;

import java.nio.file.Path;
import java.util.List;

public interface Storage {

    void store(String targetName, Path sourceFile, Permissions permissions);

    /**
     * @deprecated use method with permissions
     */
    @Deprecated
    void store(String targetName, Path sourceFile);

    void store(String name, byte[] data, String mimeType, Permissions permissions);

    /**
     * @deprecated use method with permissions
     */
    @Deprecated
    void store(String name, byte[] data, String mimeType);

    Path retrieve(String path);

    List<String> list();

    List<String> list(String path);

    void copy(String sourceName, String targetName);

    String absoluteUrl(String... paths);

    void publish(String name);
    
    void unpublish(String name);
}
