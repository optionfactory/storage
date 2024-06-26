package net.optionfactory.storage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;


public class FilesystemStorage implements Storage {

    private final Path base;

    public FilesystemStorage(Path basePath) {
        this.base = basePath;
    }

    @Override
    public void store(String name, Path file, Permissions ignored) {
        try {
            Files.createDirectories(base);
            Files.copy(file, base.resolve(name), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void store(Path target, Path sourceFile, Permissions ignored) {
        try {
            final var finalDestination = base.resolve(target);
            Files.createDirectories(finalDestination.getParent());
            Files.copy(sourceFile, finalDestination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void store(Path target, InputStream in, Permissions ignored) {
        try {
            final var finalDestination = base.resolve(target);
            Files.createDirectories(finalDestination.getParent());
            Files.copy(in, finalDestination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Avoid to use! Save the RAM save the world!
     */
    @Override
    @Deprecated
    public void store(String name, byte[] data, String mimeType, Permissions ignored) {
        try {
            Files.createDirectories(base);
            Files.write(base.resolve(name), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public InputStream retrieve(Path name) {
        try {
            return Files.newInputStream(base.resolve(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Path> list() {
        return retrieveListing(base);
    }

    @Override
    public List<Path> list(Path path) {
        return retrieveListing(base.resolve(path));
    }

    private List<Path> retrieveListing(Path path) {
        try (var list = Files.list(path)) {
            return list.toList();
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to list storage repository", ex);
        }
    }

    @Override
    public void copy(String sourceName, String targetName) {
        try {
            Files.copy(base.resolve(sourceName), base.resolve(targetName));
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
        final String path = String.join(File.pathSeparator, relativePath);
        return base.resolve(path).toUri().toString();
    }

    @Override
    public void delete(Path target) {
        final var toBeDeleted = base.resolve(target);
        if (!Files.exists(toBeDeleted)) {
            return;
        }

        if (Files.isRegularFile(toBeDeleted)) {
            try {
                Files.delete(toBeDeleted);
                return;
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to delete '%s'".formatted(target), ex);
            }
        }

        try {
            Files.walkFileTree(toBeDeleted, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
