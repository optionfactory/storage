package net.optionfactory.storage;

import java.nio.file.Path;
import java.util.List;

public interface Storage {

    void store(String targetName, Path sourceFile);

    Path retrieve(String path);

    List<String> list();

    List<String> list(String path);

    void copy(String sourceName, String targetName);

}
