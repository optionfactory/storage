package net.optionfactory.storage;

import org.apache.commons.io.IOUtils;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class S3Storage implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(S3Storage.class);
    private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";
    private static final String URL_TEMPLATE = "https://%s.s3.amazonaws.com/%s";
    private final Tika tika = new Tika();
    private final S3Client s3;
    private final String bucket;
    private final long cacheMaxAge;

    private ObjectCannedACL aclFromPermissions(Permissions permissions) {
        return switch (permissions) {
            case PUBLIC_READ -> ObjectCannedACL.PUBLIC_READ;
            case PRIVATE -> ObjectCannedACL.PRIVATE;
        };
    }

    public S3Storage(String username, String password, Region region, String bucket, long cacheMaxAge) {
        this(StaticCredentialsProvider.create(AwsBasicCredentials.create(username, password)), region, bucket, cacheMaxAge);
    }

    public S3Storage(AwsCredentialsProvider provider, Region region, String bucket, long cacheMaxAge) {
        this.s3 = S3Client.builder()
                .credentialsProvider(provider)
                .region(region)
                .build();
        this.bucket = bucket;
        this.cacheMaxAge = cacheMaxAge;
    }

    @Override
    public void store(String name, Path sourceFile, Permissions permissions) {
        try {
            final String contentType = contentTypeForFile(sourceFile);
            logger.info("Uploading to S3 file {} with content type {}", sourceFile, contentType);
            final var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .cacheControl(String.format("max-age=%d", cacheMaxAge))
                    .contentType(contentType)
                    .acl(aclFromPermissions(permissions))
                    .build();
            s3.putObject(request, sourceFile);
        } catch (SdkException ex) {
            logger.error("Unable to store {} on S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public void store(Path target, Path sourceFile, Permissions permissions) {
        store(target.toString(), sourceFile, permissions);
    }

    @Override
    public void store(Path target, InputStream in, Permissions permissions) {
        try {
            logger.info("Uploading to S3 name {}", target);
            final var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(target.toString())
                    .cacheControl(String.format("max-age=%d", cacheMaxAge))
                    .acl(aclFromPermissions(permissions))
                    .build();
            final var tempFileSoftened = createTempFileSoftened();
            try {
                in.transferTo(Files.newOutputStream(tempFileSoftened));
                s3.putObject(request, RequestBody.fromFile(tempFileSoftened));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                deleteFileSoftened(tempFileSoftened);
            }
        } catch (SdkException ex) {
            logger.error("Unable to store {} on S3 bucket {}", target, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", target, bucket), ex);
        }
    }

    /**
     * Avoid to use! Save the RAM save the world!
     */
    @Override
    @Deprecated
    public void store(String name, byte[] data, String mimeType, Permissions permissions) {
        try {
            logger.info("Uploading to S3 name {} with content type {}", name, mimeType);
            final var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .cacheControl(String.format("max-age=%d", cacheMaxAge))
                    .contentType(mimeType)
                    .acl(aclFromPermissions(permissions))
                    .build();
            s3.putObject(request, RequestBody.fromBytes(data));
        } catch (SdkException ex) {
            logger.error("Unable to store {} on S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public InputStream retrieve(Path name) {
        try {
            final var request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(name.toString())
                    .build();
            return s3.getObject(request);
        } catch (NoSuchKeyException ex) {
            throw new DataNotFoundException("Key %s not found in S3 bucket %s".formatted(name, bucket), ex);
        } catch (SdkException ex) {
            logger.error("Unable to retrieve {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException("Unable to retrieve %s from S3 bucket %s".formatted(name, bucket), ex);
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

    @Override
    public void copy(Path sourceName, Path targetName) {
        try {
            final var request = CopyObjectRequest.builder()
                    .sourceBucket(bucket)
                    .destinationBucket(bucket)
                    .sourceKey(sourceName.toString())
                    .destinationKey(targetName.toString())
                    .build();
            s3.copyObject(request);
        } catch (NoSuchKeyException ex) {
            throw new DataNotFoundException("Key %s not found in S3 bucket %s".formatted(sourceName, bucket), ex);
        } catch (SdkException ex) {
            logger.error("Unable to copy {} to {} from S3 bucket {}", sourceName, targetName, bucket, ex);
            throw new IllegalStateException(String.format("Unable to copy %s to %s from S3 bucket %s", sourceName, targetName, bucket), ex);
        }
    }

    @Override
    public void publish(String name) {
        try {
            final var request = PutObjectAclRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .acl(ObjectCannedACL.PUBLIC_READ)
                    .build();
            s3.putObjectAcl(request);
        } catch (SdkException ex) {
            logger.error("Unable to publish {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to publish %s from S3 bucket %s", name, bucket), ex);
        }

    }

    @Override
    public void unpublish(String name) {
        try {
            final var request = PutObjectAclRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .acl(ObjectCannedACL.PRIVATE)
                    .build();
            s3.putObjectAcl(request);
        } catch (SdkException ex) {
            logger.error("Unable to unpublish {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to unpublish %s from S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public List<Path> list() {
        final var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .build();
        final var listObjectsResponse = s3.listObjectsV2Paginator(request);
        return adaptKeyToPath(listObjectsResponse);
    }

    @Override
    public List<Path> list(Path prefix) {
        final var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix.toString())
                .build();
        final var listObjectsResponse = s3.listObjectsV2Paginator(request);
        return adaptKeyToPath(listObjectsResponse);
    }

    private List<Path> adaptKeyToPath(ListObjectsV2Iterable objects) {
        return objects.stream()
                .flatMap(r -> r.contents().stream())
                .map(S3Object::key)
                .map(Path::of)
                .toList();
    }

    private String contentTypeForFile(Path file) {
        try {
            return Optional.ofNullable(tika.detect(file))
                    .orElse(DEFAULT_CONTENT_TYPE);
        } catch (IOException ex) {
            logger.error("Unable to determine MIME type for file {}", file.toString(), ex);
            return DEFAULT_CONTENT_TYPE;
        }
    }

    @Override
    public String absoluteUrl(String... relativePath) {
        if (relativePath.length == 0) {
            throw new IllegalArgumentException("At least one relative path must be specified.");
        }
        return String.format(URL_TEMPLATE, bucket, String.join("/", relativePath));
    }

    @Override
    public void delete(Path target) {
        try {
            final var toBeDeleted = list(target)
                    .stream()
                    .map(element -> ObjectIdentifier.builder().key(element.toString()).build())
                    .toList();
            final var delete = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(builder -> builder.objects(toBeDeleted))
                    .build();
            s3.deleteObjects(delete);
        } catch (SdkException ex) {
            logger.error("Unable to delete {} from S3 bucket {}", target, bucket, ex);
            throw new IllegalStateException(String.format("Unable to retrieve %s from S3 bucket %s", target, bucket), ex);
        }
    }

    private Path createTempFileSoftened() {
        try {
            return Files.createTempFile("storage-", ".tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteFileSoftened(Path toBeDeleted) {
        try {
            Files.deleteIfExists(toBeDeleted);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
