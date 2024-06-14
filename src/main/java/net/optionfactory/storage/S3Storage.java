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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
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
    public void store(String targetName, Path sourceFile, Permissions permissions) {
        try {
            final String contentType = contentTypeForFile(sourceFile);
            logger.info("Uploading to S3 file {} with content type {}", sourceFile, contentType);
            final var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(targetName)
                    .cacheControl(String.format("max-age=%d", cacheMaxAge))
                    .contentType(contentType)
                    .acl(aclFromPermissions(permissions))
                    .build();
            s3.putObject(request, sourceFile);
        } catch (SdkException ex) {
            logger.error("Unable to store {} on S3 bucket {}", targetName, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", targetName, bucket), ex);
        }
    }

    @Override
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
    public Path retrieve(String name) {
        try {
            final var request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .build();

            try (final var is = s3.getObject(request)) {
                final Path temp = Files.createTempFile(bucket, ".tmp");
                try (final OutputStream os = Files.newOutputStream(temp)) {
                    IOUtils.copy(is, os);
                    return temp;
                }
            }
        } catch (SdkException | IOException ex) {
            logger.error("Unable to retrieve {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to retrieve %s from S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public void copy(String sourceName, String targetName) {
        try {
            final var request = CopyObjectRequest.builder()
                    .sourceBucket(bucket)
                    .destinationBucket(bucket)
                    .sourceKey(sourceName)
                    .destinationKey(targetName)
                    .build();
            s3.copyObject(request);
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
    public List<String> list() {
        final var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .build();
        final var listObjectsResponse = s3.listObjectsV2Paginator(request);
        return retrieveAll(listObjectsResponse);
    }

    @Override
    public List<String> list(String prefix) {
        final var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        final var listObjectsResponse = s3.listObjectsV2Paginator(request);
        return retrieveAll(listObjectsResponse);
    }

    private List<String> retrieveAll(ListObjectsV2Iterable objects) {
        return objects.stream()
                .flatMap(r -> r.contents().stream())
                .map(S3Object::key)
                .toList();
    }

    public String contentTypeForFile(Path file) {
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

}
