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

public class Zipper {
    private final static Logger logger = LoggerFactory.getLogger(Zipper.class);

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

    public static byte[] compress(Map<String, ? extends InputStream> filenamesAndStreams) throws IOException {
        logger.debug("Compress in memory");
        try (final var baos = new ByteArrayOutputStream();
             var zout = new ZipOutputStream(baos)) {
            for (Map.Entry<String, ? extends InputStream> entry : filenamesAndStreams.entrySet()) {
                final var name = entry.getKey();
                final var stream = entry.getValue();
                logger.debug("Adding file {} to zip", name);
                zout.putNextEntry(new ZipEntry(name));
                stream.transferTo(zout);
                zout.closeEntry();
            }
            return baos.toByteArray();
        }
    }
}
