package net.optionfactory.storage;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.cloud.storage.Storage.BlobListOption;
import static com.google.cloud.storage.Storage.BlobWriteOption;
import static com.google.cloud.storage.Storage.CopyRequest;
import static com.google.cloud.storage.Storage.PredefinedAcl;

public class GcpStorage implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(GcpStorage.class);

    private final String bucket;
    private final com.google.cloud.storage.Storage storage;
    private final boolean uniformBucketLevelAccess;

    public GcpStorage(String projectId, String bucketId, boolean uniformBucketLevelAccess) {
        this.bucket = bucketId;
        this.storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        this.uniformBucketLevelAccess = uniformBucketLevelAccess;
    }

    @Override
    public void store(String name, Path sourceFile, Permissions permissions) {
        try {
            final var blobId = BlobId.of(bucket, name);
            final var blobWriteOption = Optional.ofNullable(storage.get(bucket, name))
                    .map(o -> BlobWriteOption.generationMatch(o.getGeneration()))
                    .orElse(BlobWriteOption.doesNotExist());
            if (uniformBucketLevelAccess) {
                storage.createFrom(BlobInfo.newBuilder(blobId).build(), sourceFile, blobWriteOption);
                return;
            }
            final var aclOption = switch (permissions) {
                case PUBLIC_READ -> BlobWriteOption.predefinedAcl(PredefinedAcl.PUBLIC_READ);
                case PRIVATE -> BlobWriteOption.predefinedAcl(PredefinedAcl.PRIVATE);
            };
            storage.createFrom(BlobInfo.newBuilder(blobId).build(), sourceFile, blobWriteOption, aclOption);
        } catch (IOException | StorageException ex) {
            logger.error("Unable to store {} on GCP bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on GCP bucket %s", name, bucket), ex);
        }
    }

    @Override
    public void store(Path target, Path sourceFile, Permissions permissions) {
        store(target.toString(), sourceFile, permissions);
    }

    @Override
    public void store(Path target, InputStream in, Permissions permissions) {
        final var name = target.toString();
        try {
            final var blobId = BlobId.of(bucket, name);
            final var blobWriteOption = Optional.ofNullable(storage.get(bucket, name))
                    .map(o -> BlobWriteOption.generationMatch(o.getGeneration()))
                    .orElse(BlobWriteOption.doesNotExist());
            if (uniformBucketLevelAccess) {
                storage.createFrom(BlobInfo.newBuilder(blobId).build(), in, blobWriteOption);
                return;
            }
            final var aclOption = switch (permissions) {
                case PUBLIC_READ -> BlobWriteOption.predefinedAcl(PredefinedAcl.PUBLIC_READ);
                case PRIVATE -> BlobWriteOption.predefinedAcl(PredefinedAcl.PRIVATE);
            };
            storage.createFrom(BlobInfo.newBuilder(blobId).build(), in, blobWriteOption, aclOption);
        } catch (IOException | StorageException ex) {
            logger.error("Unable to store {} on GCP bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on GCP bucket %s", name, bucket), ex);
        }
    }

    @Override
    @Deprecated
    public void store(String name, byte[] data, String mimeType, Permissions permissions) {
        throw new UnsupportedOperationException("Deprecated");
    }

    @Override
    public InputStream retrieve(Path path) {
        final var name = path.toString();
        try {
            final var blob = Optional.ofNullable(storage.get(bucket, name))
                    .orElseThrow(() -> new DataNotFoundException("Key %s not found in GCP bucket %s".formatted(name, bucket)));
            return Channels.newInputStream(blob.reader());
        } catch (StorageException ex) {
            logger.error("Unable to retrieve {} from GCP bucket {}", name, bucket, ex);
            throw new IllegalStateException("Unable to retrieve %s from GCP bucket %s".formatted(name, bucket), ex);
        }
    }

    @Override
    public Path cacheLocally(InputStream inputStream) {
        try {
            final Path temp = Files.createTempFile(bucket, ".tmp");
            try (final OutputStream os = Files.newOutputStream(temp)) {
                IOUtils.copy(inputStream, os);
                return temp;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * List not recursive, same as searching with path '/'
     */
    @Override
    public List<Path> list() {
        final var cd = BlobListOption.currentDirectory();
        return storage.list(bucket, cd)
                .streamAll()
                .map(BlobInfo::getName)
                .map(Path::of)
                .toList();
    }

    /**
     * List not recursive, resulting paths have the base prefix i.e., search 'a/' -> 'a/b'
     */
    @Override
    public List<Path> list(Path path) {
        final var prefix = "%s/".formatted(path);
        final var cd = BlobListOption.currentDirectory();
        return storage.list(bucket, BlobListOption.prefix("%s/".formatted(path)), cd)
                .streamAll()
                .map(BlobInfo::getName)
                .filter(n -> !prefix.equalsIgnoreCase(n))
                .map(Path::of)
                .toList();
    }

    @Override
    public void copy(Path sourceName, Path targetName) {
        final var target = targetName.toString();
        try {
            final var blobWriteOption = Optional.ofNullable(storage.get(bucket, target))
                    .map(o -> BlobTargetOption.generationMatch(o.getGeneration()))
                    .orElse(BlobTargetOption.doesNotExist());
            final var req = CopyRequest.newBuilder()
                    .setSource(bucket, sourceName.toString())
                    .setTarget(BlobId.of(bucket, target), blobWriteOption)
                    .build();
            storage.copy(req);
        } catch (StorageException ex) {
            logger.error("Unable to copy {} to {} in GCP bucket {}", sourceName, targetName, bucket, ex);
            throw new IllegalStateException("Unable to copy %s to %s in GCP bucket %s".formatted(sourceName, targetName, bucket), ex);
        }
    }

    @Override
    public String absoluteUrl(String... paths) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void delete(Path target) {
        // Deletes the blob specified by its id. When the generation is present and non-null it will be
        // specified in the request.
        // If versioning is enabled on the bucket and the generation is present in the delete request,
        // only the version of the object with the matching generation will be deleted.
        try {
            Optional.ofNullable(storage.get(bucket, target.toString()))
                    .ifPresent(blob -> storage.delete(blob.getBlobId()));
        } catch (StorageException ex) {
            logger.error("Unable to delete {} from GCP bucket {}", target, bucket, ex);
            throw new IllegalStateException(String.format("Unable to delete %s from GCP bucket %s", target, bucket), ex);
        }
    }

    @Override
    public void publish(String name) {
        if (uniformBucketLevelAccess) {
            return;
        }
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void unpublish(String name) {
        if (uniformBucketLevelAccess) {
            return;
        }
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
