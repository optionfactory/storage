package net.optionfactory.storage;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        final var decompressedFiles = new ArrayList<Path>();
        logger.debug("Decompressing file '{}'", zippedFile);

        try (final var zip = ZipFile.builder().setFile(zippedFile.toFile()).get()) {
            final var entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                final var entry = entries.nextElement();
                if (entry.isDirectory()) {
                    continue;
                }
                final var output = dst.resolve(entry.getName());
                if (output.toFile().exists()) {
                    logger.debug("File '{}' already exists...", output);
                    decompressedFiles.add(output);
                    continue;
                }
                logger.debug("Creating file '{}'", output);
                Files.createDirectories(output.getParent());
                final var outputFile = Files.createFile(output);
                final var fileOutputStream = new FileOutputStream(outputFile.toFile());
                zip.getInputStream(entry).transferTo(fileOutputStream);
                fileOutputStream.close();
                decompressedFiles.add(outputFile);
            }
            final var count = decompressedFiles.size();
            logger.debug("Decompressed {} files", count);
            return decompressedFiles;
        } catch (IOException e) {
            throw new RuntimeException("Error decompressing file", e);
        }
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
        final var files = fileNames.stream()
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .toList();
        logger.debug("Compressing into file '{}'", dst);
        final var outputFile = dst.toFile();
        try (final var fout = new FileOutputStream(outputFile);
             final var zout = new ZipOutputStream(fout)) {
            for (final Path file : files) {
                final var fileName = file.getFileName();
                logger.debug("Adding file {} to zip", fileName);
                zout.putNextEntry(new ZipEntry(fileName.toString()));
                Files.copy(file, zout);
                zout.closeEntry();
            }
        }
        logger.debug("Compressed {} files", files.size());
        return dst;
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
            try (var zout = new ZipOutputStream(baos)) {
                for (Map.Entry<String, ? extends InputStream> entry : filenamesAndStreams.entrySet()) {
                    final var name = entry.getKey();
                    final var stream = entry.getValue();
                    logger.debug("Adding file {} to zip", name);
                    zout.putNextEntry(new ZipEntry(name));
                    stream.transferTo(zout);
                    zout.closeEntry();
                }
            }
            return baos.toByteArray();
        }
    }
}
