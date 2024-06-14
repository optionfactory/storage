package net.optionfactory.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import org.apache.tika.Tika;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3Storage implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(S3Storage.class);
    private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";
    private static final String URL_TEMPLATE = "https://%s.s3.amazonaws.com/%s";
    private final Tika tika = new Tika();
    private final AmazonS3 s3;
    private final String bucket;
    private final long cacheMaxAge;

    private CannedAccessControlList aclFromPermissions(Permissions permissions) {
        return switch (permissions) {
            case PUBLIC_READ -> CannedAccessControlList.PublicRead;
            case PRIVATE -> CannedAccessControlList.Private;
        };
    }

    public S3Storage(String username, String password, String region, String bucket, long cacheMaxAge) {
        this(new AWSStaticCredentialsProvider(new BasicAWSCredentials(username, password)), region, bucket, cacheMaxAge);
    }

    public S3Storage(AWSCredentialsProvider provider, String region, String bucket, long cacheMaxAge) {
        this.s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(provider)
                .withRegion(region)
                .build();
        this.bucket = bucket;
        this.cacheMaxAge = cacheMaxAge;
    }

    @Override
    public void store(String targetName, Path sourceFile, Permissions permissions) {
        try {
            final PutObjectRequest por = new PutObjectRequest(bucket, targetName, sourceFile.toFile());
            final String contentType = contentTypeForFile(sourceFile);
            logger.info("Uploading to S3 file {} with content type {}", sourceFile, contentType);
            por.putCustomRequestHeader("Content-Type", contentType);
            por.putCustomRequestHeader("Cache-Control", String.format("max-age=%d", cacheMaxAge));
            por.setCannedAcl(aclFromPermissions(permissions));
            s3.putObject(por);
        } catch (AmazonClientException ex) {
            logger.error("Unable to store {} on S3 bucket {}", targetName, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", targetName, bucket), ex);
        }
    }

    @Override
    public void store(String name, byte[] data, String mimeType, Permissions permissions) {
        try {
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            metadata.setContentType(mimeType);
            final PutObjectRequest por = new PutObjectRequest(bucket, name, new ByteArrayInputStream(data), metadata);
            logger.info("Uploading to S3 name {} with content type {}", name, mimeType);
            por.putCustomRequestHeader("Content-Type", mimeType);
            por.putCustomRequestHeader("Cache-Control", String.format("max-age=%d", cacheMaxAge));
            por.setCannedAcl(aclFromPermissions(permissions));
            s3.putObject(por);
        } catch (AmazonClientException ex) {
            logger.error("Unable to store {} on S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public Path retrieve(String name) {
        try {
            final S3Object resource = s3.getObject(bucket, name);
            try (final InputStream is = resource.getObjectContent()) {
                final Path temp = Files.createTempFile(bucket, ".tmp");
                try (final OutputStream os = Files.newOutputStream(temp)) {
                    IOUtils.copy(is, os);
                    return temp;
                }
            }
        } catch (AmazonClientException | IOException ex) {
            logger.error("Unable to retrieve {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to retrieve %s from S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public void copy(String sourceName, String targetName) {
        try {
            s3.copyObject(bucket, sourceName, bucket, targetName);
        } catch (AmazonClientException ex) {
            logger.error("Unable to copy {} to {} from S3 bucket {}", sourceName, targetName, bucket, ex);
            throw new IllegalStateException(String.format("Unable to copy %s to %s from S3 bucket %s", sourceName, targetName, bucket), ex);
        }
    }

    @Override
    public void publish(String name) {
        try {
            s3.setObjectAcl(bucket, name, CannedAccessControlList.PublicRead);
        } catch (AmazonClientException ex) {
            logger.error("Unable to publish {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to publish %s from S3 bucket %s", name, bucket), ex);
        }

    }

    @Override
    public void unpublish(String name) {
        try {
            s3.setObjectAcl(bucket, name, CannedAccessControlList.Private);
        } catch (AmazonClientException ex) {
            logger.error("Unable to unpublish {} from S3 bucket {}", name, bucket, ex);
            throw new IllegalStateException(String.format("Unable to unpublish %s from S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public List<String> list() {
        final ObjectListing listObjects = s3.listObjects(bucket);
        return retrieveListing(listObjects);
    }

    @Override
    public List<String> list(String path) {
        final ObjectListing listObjects = s3.listObjects(bucket, path);
        return retrieveListing(listObjects);
    }

    private List<String> retrieveListing(ObjectListing request) {
        final List<String> contents = new ArrayList<>();
        request.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(() -> contents));
        while (request.isTruncated()) {
            request = s3.listNextBatchOfObjects(request);
            request.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(() -> contents));
        }
        return contents;
    }

    public String contentTypeForFile(Path file) {
        try {
            return Optional.ofNullable(tika.detect(file))
                    .orElse(DEFAULT_CONTENT_TYPE);
        } catch (IOException ex) {
            logger.error("Unable to dermine MIME type for file {}", file.toString(), ex);
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
