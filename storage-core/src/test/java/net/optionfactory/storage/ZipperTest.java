package net.optionfactory.storage;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    public void canDecompressFromInputStream() throws IOException {
        final var base = Files.createTempDirectory("zipper-is");
        final var fs = new FilesystemStorage(base);
        for (int i = 0; i < 5; i++) {
            final var fileName = i + "_a_file.dat";
            final var is = new ByteArrayInputStream("Test content".getBytes(StandardCharsets.UTF_8));
            fs.store(Paths.get(fileName), is, null);
        }
        final var compressed = Zipper.compress(fs.list(), base.resolve("zipped.zip"));
        
        final var dst = Files.createTempDirectory("decompressed-is");
        try (final var is = Files.newInputStream(compressed)) {
            final var decompressed = Zipper.decompress(dst, is);
            Assertions.assertEquals(5, decompressed.size());
        }
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

    @Test
    public void canCompressToOutputStream() throws IOException {
        final var base = Files.createTempDirectory("zipper-os");
        final var file1 = Files.writeString(base.resolve("test1.txt"), "Content 1");
        final var file2 = Files.writeString(base.resolve("test2.txt"), "Content 2");

        final var baos = new ByteArrayOutputStream();
        Zipper.compress(List.of(file1, file2), baos);

        final var zipBytes = baos.toByteArray();
        Assertions.assertTrue(zipBytes.length > 0);

        final var decompressed = Zipper.decompress(new ByteArrayInputStream(zipBytes));
        Assertions.assertEquals(2, decompressed.size());
        Assertions.assertArrayEquals("Content 1".getBytes(StandardCharsets.UTF_8), decompressed.get("test1.txt"));
        Assertions.assertArrayEquals("Content 2".getBytes(StandardCharsets.UTF_8), decompressed.get("test2.txt"));
    }

    @Test
    public void canDecompressWithConsumer() throws IOException {
        final var input = Map.of("data.txt", new ByteArrayInputStream("Hello Consumer".getBytes(StandardCharsets.UTF_8)));
        final var zipBytes = Zipper.compress(input);

        final var result = new ArrayList<String>();
        Zipper.decompress(new ByteArrayInputStream(zipBytes), (entry, is) -> {
            try {
                result.add(entry.getName() + ":" + new String(is.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("data.txt:Hello Consumer", result.get(0));
    }

    @Test
    public void canDecompressInMemory() throws IOException {
        final var input = Map.of("m1.txt", new ByteArrayInputStream("Memory 1".getBytes(StandardCharsets.UTF_8)),
                                 "m2.txt", new ByteArrayInputStream("Memory 2".getBytes(StandardCharsets.UTF_8)));
        final var zipBytes = Zipper.compress(input);

        final var decompressed = Zipper.decompress(new ByteArrayInputStream(zipBytes));

        Assertions.assertEquals(2, decompressed.size());
        Assertions.assertArrayEquals("Memory 1".getBytes(StandardCharsets.UTF_8), decompressed.get("m1.txt"));
        Assertions.assertArrayEquals("Memory 2".getBytes(StandardCharsets.UTF_8), decompressed.get("m2.txt"));
    }
}
