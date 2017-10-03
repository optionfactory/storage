package net.optionfactory.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class FilesystemStorage implements Storage {

    private final Path repository;

    public FilesystemStorage(String repositoryPath) {
        this.repository = Paths.get(repositoryPath);
    }

    @Override
    public void store(String name, Path file) {
        try {
            Files.createDirectories(repository);
            Files.copy(file, repository.resolve(name));
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

}
