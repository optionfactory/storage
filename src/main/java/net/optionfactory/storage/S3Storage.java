package net.optionfactory.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.apache.tika.Tika;

public class S3Storage implements Storage {

    private static final Logger logger = Logger.getLogger(S3Storage.class);
    private static final String DEFAULT_CONTENT_TYPE = "binary/octet-stream";
    private static final String URL_TEMPLATE = "https://%s.s3.amazonaws.com/%s";
    private final Tika tika = new Tika();
    private final AmazonS3 s3;
    private final String bucket;
    private final long cacheMaxAge;

    private CannedAccessControlList aclFromPermissions(Permissions permissions) {
        switch (permissions) {
            case PUBLIC_READ:
                return CannedAccessControlList.PublicRead;
            case PRIVATE:
                return CannedAccessControlList.Private;
            default:
                throw new IllegalArgumentException(String.format("Unknown permission %s", permissions));

        }
    }

    public S3Storage(String username, String password, String region, String bucket, long cacheMaxAge) {
        final AWSCredentials credentials = new BasicAWSCredentials(username, password);
        this.s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();
        this.bucket = bucket;
        this.cacheMaxAge = cacheMaxAge;
    }

    @Override
    public void store(String targetName, Path sourceFile) {
        store(targetName, sourceFile, Permissions.PUBLIC_READ);
    }

    @Override
    public void store(String targetName, Path sourceFile, Permissions permissions) {
        try {
            final PutObjectRequest por = new PutObjectRequest(bucket, targetName, sourceFile.toFile());
            final String contentType = contentTypeForFile(sourceFile);
            logger.info(String.format("Uploading to S3 file %s with content type %s", sourceFile.toString(), contentType));
            por.putCustomRequestHeader("Content-Type", contentType);
            por.putCustomRequestHeader("Cache-Control", String.format("max-age=%d", cacheMaxAge));
            por.setCannedAcl(aclFromPermissions(permissions));
            s3.putObject(por);
        } catch (AmazonClientException ex) {
            logger.error(String.format("Unable to store %s on S3 bucket %s", targetName, bucket), ex);
            throw new IllegalStateException(String.format("Unable to store %s on S3 bucket %s", targetName, bucket), ex);
        }
    }

    @Override
    public void store(String name, byte[] data, String mimeType) {
        store(name, data, mimeType, Permissions.PUBLIC_READ);
    }

    @Override
    public void store(String name, byte[] data, String mimeType, Permissions permissions) {
        try {
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            metadata.setContentType(mimeType);
            final PutObjectRequest por = new PutObjectRequest(bucket, name, new ByteArrayInputStream(data), metadata);
            logger.info(String.format("Uploading to S3 name %s with content type %s", name, mimeType));
            por.putCustomRequestHeader("Content-Type", mimeType);
            por.putCustomRequestHeader("Cache-Control", String.format("max-age=%d", cacheMaxAge));
            por.setCannedAcl(aclFromPermissions(permissions));
            s3.putObject(por);
        } catch (AmazonClientException ex) {
            logger.error(String.format("Unable to store %s on S3 bucket %s", name, bucket), ex);
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
            logger.error(String.format("Unable to retrieve %s from S3 bucket %s", name, bucket), ex);
            throw new IllegalStateException(String.format("Unable to retrieve %s from S3 bucket %s", name, bucket), ex);
        }
    }

    @Override
    public void copy(String sourceName, String targetName) {
        try {
            s3.copyObject(bucket, sourceName, bucket, targetName);
        } catch (AmazonClientException ex) {
            logger.error(String.format("Unable to copy %s to %s from S3 bucket %s", sourceName, targetName, bucket), ex);
            throw new IllegalStateException(String.format("Unable to copy %s to %s from S3 bucket %s", sourceName, targetName, bucket), ex);
        }
    }

    @Override
    public void publish(String name) {
        try {
            s3.setObjectAcl(bucket, name, CannedAccessControlList.PublicRead);
        } catch (AmazonClientException ex) {
            logger.error(String.format("Unable to publish %s from S3 bucket %s", name, bucket), ex);
            throw new IllegalStateException(String.format("Unable to publish %s from S3 bucket %s", name, bucket), ex);
        }

    }

    @Override
    public void unpublish(String name) {
        try {
            s3.setObjectAcl(bucket, name, CannedAccessControlList.Private);
        } catch (AmazonClientException ex) {
            logger.error(String.format("Unable to unpublish %s from S3 bucket %s", name, bucket), ex);
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
        request.getObjectSummaries().stream().map(o -> o.getKey()).collect(Collectors.toCollection(() -> contents));
        while (request.isTruncated()) {
            request = s3.listNextBatchOfObjects(request);
            request.getObjectSummaries().stream().map(o -> o.getKey()).collect(Collectors.toCollection(() -> contents));
        }
        return contents;
    }

    public String contentTypeForFile(Path file) {
        try {
            return Optional.ofNullable(tika.detect(file))
                    .orElse(DEFAULT_CONTENT_TYPE);
        } catch (IOException ex) {
            logger.error(String.format("Unable to dermine MIME type for file %s", file.toString()), ex);
            return DEFAULT_CONTENT_TYPE;
        }
    }

    @Override
    public String absoluteUrl(String... relativePath) {
        if (relativePath.length == 0) {
            throw new IllegalArgumentException("At least one relative path must be specified.");
        }
        return String.format(URL_TEMPLATE, bucket, Stream.of(relativePath).collect(Collectors.joining("/")));
    }

}
