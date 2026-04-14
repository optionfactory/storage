package net.optionfactory.storage;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ZipperTest {

    public static final String LONG_LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismodLorem ipsum dolor sit amet, consectetur adipiscing elit. In ut ante euismod";

    @Test
    public void canCompressABunchOfFiles() throws IOException {
        final var base = Files.createTempDirectory("zipper");
        final var fs = new FilesystemStorage(base);
        var fileNames = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            final var fileName = i + "_a_file.dat";
            fileNames.add(fileName);
            final var is = new ByteArrayInputStream(LONG_LOREM.getBytes(StandardCharsets.UTF_8));
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

    @Test
    public void canCompressInMemory() throws IOException {
        final var input = IntStream.range(0, 100)
                .mapToObj(i -> Pair.of("file_%s.txt".formatted(i), new ByteArrayInputStream(LONG_LOREM.getBytes(StandardCharsets.UTF_8))))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        var uncompressedBytes = LONG_LOREM.getBytes(StandardCharsets.UTF_8).length * 100;
        final var compress = Zipper.compress(input);
        Assertions.assertTrue(uncompressedBytes > compress.length, "compressed size should be less than uncompressed");
    }
}
