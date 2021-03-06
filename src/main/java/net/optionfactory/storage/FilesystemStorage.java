package net.optionfactory.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FilesystemStorage implements Storage {

    private final Path repository;

    public FilesystemStorage(String repositoryPath) {
        this.repository = Paths.get(repositoryPath);
    }

    @Override
    public void store(String name, Path file) {
        store(name, file, null);

    }

    @Override
    public void store(String name, Path file, Permissions ignored) {
        try {
            Files.createDirectories(repository);
            Files.copy(file, repository.resolve(name));
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void store(String name, byte[] data, String mimeType) {
        store(name, data, mimeType, null);
    }

    @Override
    public void store(String name, byte[] data, String mimeType, Permissions ignored) {
        try {
            Files.createDirectories(repository);
            Files.write(repository.resolve(name), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public Path retrieve(String name) {
        return repository.resolve(name);
    }

    @Override
    public List<String> list() {
        return retrieveListing(repository);
    }

    @Override
    public List<String> list(String path) {
        return retrieveListing(repository.resolve(path));
    }

    private List<String> retrieveListing(Path path) {
        try {
            return Files.list(path)
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to list storage repository", ex);
        }
    }

    @Override
    public void copy(String sourceName, String targetName) {
        try {
            Files.copy(repository.resolve(sourceName), repository.resolve(targetName));
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to copy file", ex);
        }
    }

    @Override
    public String absoluteUrl(String... relativePath) {
        if (relativePath.length == 0) {
            throw new IllegalArgumentException("At least one relative path must be specified.");
        }
        //TODO: override uri generation with a pre-defined schema/domain if files are served somehow
        final String path = Stream.of(relativePath).collect(Collectors.joining(File.pathSeparator));
        return repository.resolve(path).toUri().toString();
    }

    @Override
    public void publish(String name) {
        // noop
    }

    @Override
    public void unpublish(String name) {
        // noop
    }

}
