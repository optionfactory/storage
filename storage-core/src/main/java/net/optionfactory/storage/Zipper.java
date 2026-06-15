package net.optionfactory.storage;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Utility class for compressing and decompressing ZIP files.
 */
public class Zipper {
    private final static Logger logger = LoggerFactory.getLogger(Zipper.class);

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private Zipper() {
    }

    /**
     * Decompresses a ZIP file to a destination directory.
     *
     * @param dst        the destination directory
     * @param zippedFile the source ZIP file
     * @return a list of paths to the decompressed files
     * @throws RuntimeException if decompression fails
     */
    public static List<Path> decompress(Path dst, Path zippedFile) {
        try (final var is = new FileInputStream(zippedFile.toFile())) {
            return decompress(dst, is);
        } catch (IOException e) {
            throw new RuntimeException("Error decompressing file", e);
        }
    }

    /**
     * Decompresses a ZIP input stream to a destination directory.
     *
     * @param dst        the destination directory
     * @param zippedFile the source ZIP input stream
     * @return a list of paths to the decompressed files
     * @throws RuntimeException if decompression fails
     */
    public static List<Path> decompress(Path dst, InputStream zippedFile) {
        final var decompressedFiles = new ArrayList<Path>();
        decompress(zippedFile, (entry, is) -> {
            try {
                final var output = dst.resolve(entry.getName());
                if (output.toFile().exists()) {
                    logger.debug("File '{}' already exists...", output);
                    decompressedFiles.add(output);
                    return;
                }
                logger.debug("Creating file '{}'", output);
                Files.createDirectories(output.getParent());
                final var outputFile = Files.createFile(output);
                try (final var fileOutputStream = new FileOutputStream(outputFile.toFile())) {
                    is.transferTo(fileOutputStream);
                }
                decompressedFiles.add(outputFile);
            } catch (IOException e) {
                throw new RuntimeException("Error writing decompressed file", e);
            }
        });
        final var count = decompressedFiles.size();
        logger.debug("Decompressed {} files", count);
        return decompressedFiles;
    }

    /**
     * Decompresses a ZIP input stream using a consumer for each entry.
     *
     * @param zippedFile the source ZIP input stream
     * @param consumer   a consumer that receives the entry and its input stream
     * @throws RuntimeException if decompression fails
     */
    public static void decompress(InputStream zippedFile, BiConsumer<ZipArchiveEntry, InputStream> consumer) {
        try (final var zip = new ZipArchiveInputStream(zippedFile)) {
            ZipArchiveEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                consumer.accept(entry, zip);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error decompressing stream", e);
        }
    }

    /**
     * Decompresses a ZIP input stream into a map of file names and contents.
     *
     * @param zippedFile the source ZIP input stream
     * @return a map where keys are file names and values are byte arrays of the file contents
     * @throws RuntimeException if decompression fails
     */
    public static Map<String, byte[]> decompress(InputStream zippedFile) {
        final var result = new HashMap<String, byte[]>();
        decompress(zippedFile, (entry, is) -> {
            try {
                result.put(entry.getName(), is.readAllBytes());
            } catch (IOException e) {
                throw new RuntimeException("Error reading ZIP entry into memory", e);
            }
        });
        return result;
    }

    /**
     * Compresses a list of files into a ZIP archive at the specified destination.
     *
     * @param fileNames the list of paths to the files to compress
     * @param dst       the destination path for the ZIP archive
     * @return the path to the created ZIP archive
     * @throws IOException if compression fails
     */
    public static Path compress(List<Path> fileNames, Path dst) throws IOException {
        logger.debug("Compressing into file '{}'", dst);
        try (final var fout = new FileOutputStream(dst.toFile())) {
            compress(fileNames, fout);
        }
        return dst;
    }

    /**
     * Compresses a list of files into a ZIP archive written to the provided OutputStream.
     *
     * @param fileNames the list of paths to the files to compress
     * @param os        the destination OutputStream
     * @throws IOException if compression fails
     */
    public static void compress(List<Path> fileNames, OutputStream os) throws IOException {
        final var files = fileNames.stream()
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .toList();
        try (final var zout = new ZipOutputStream(os)) {
            for (final Path file : files) {
                final var fileName = file.getFileName();
                logger.debug("Adding file {} to zip", fileName);
                zout.putNextEntry(new ZipEntry(fileName.toString()));
                Files.copy(file, zout);
                zout.closeEntry();
            }
        }
        logger.debug("Compressed {} files", files.size());
    }

    /**
     * Compresses multiple input streams into a ZIP archive in memory.
     *
     * @param filenamesAndStreams a map where keys are file names and values are input streams
     * @return a byte array containing the ZIP archive data
     * @throws IOException if compression fails
     */
    public static byte[] compress(Map<String, ? extends InputStream> filenamesAndStreams) throws IOException {
        logger.debug("Compress in memory");
        try (var baos = new ByteArrayOutputStream()) {
            compress(filenamesAndStreams, baos);
            return baos.toByteArray();
        }
    }

    /**
     * Compresses multiple input streams into a ZIP archive written to the provided OutputStream.
     *
     * @param filenamesAndStreams a map where keys are file names and values are input streams
     * @param os                  the destination OutputStream
     * @throws IOException if compression fails
     */
    public static void compress(Map<String, ? extends InputStream> filenamesAndStreams, OutputStream os) throws IOException {
        try (var zout = new ZipOutputStream(os)) {
            for (Map.Entry<String, ? extends InputStream> entry : filenamesAndStreams.entrySet()) {
                final var name = entry.getKey();
                final var stream = entry.getValue();
                logger.debug("Adding file {} to zip", name);
                zout.putNextEntry(new ZipEntry(name));
                stream.transferTo(zout);
                zout.closeEntry();
            }
        }
    }
}
