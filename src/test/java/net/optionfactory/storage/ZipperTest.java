package net.optionfactory.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

class ZipperTest {
    @Test
    public void canCompressABunchOfFiles() throws IOException {
        final var base = Files.createTempDirectory("zipper");
        final var fs = new FilesystemStorage(base);
        var fileNames = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            final var fileName = i + "_a_file.dat";
            fileNames.add(fileName);
            final var is = new ByteArrayInputStream("Lorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismod".getBytes(StandardCharsets.UTF_8));
            fs.store(Paths.get(fileName), is, null);
        }
        long totalSize = 0;
        for (Path file : fs.list()) {
            totalSize += Files.size(file);
        }
        final var compressed = Zipper.compress(fs.list(), base.resolve("zipped.zip"));
        final var size = Files.size(compressed);
        Assertions.assertTrue(size < totalSize);
    }

    @Test
    public void canDecompressABunchOfFiles() throws IOException {
        final var base = Files.createTempDirectory("zipper");
        final var fs = new FilesystemStorage(base);
        var fileNames = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            final var fileName = i + "_a_file.dat";
            fileNames.add(fileName);
            final var is = new ByteArrayInputStream("Lorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismod".getBytes(StandardCharsets.UTF_8));
            fs.store(Paths.get(fileName), is, null);
        }
        final var compressed = Zipper.compress(fs.list(), base.resolve("zipped.zip"));
        final var decompressed = Zipper.decompress(base, compressed);
        Assertions.assertEquals(10, decompressed.size());
    }
}