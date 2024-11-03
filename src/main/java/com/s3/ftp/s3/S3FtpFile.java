package com.s3.ftp.s3;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class S3FtpFile implements FtpFile {

    private final S3Client client;

    private final String bucket;

    private final String key;

    private final User user;

    private Boolean exist;

    private Long lastModified;

    private Long size;

    public S3FtpFile(S3Client client, String bucket, String key, User user) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.user = user;
    }

    @Override
    public String getAbsolutePath() {
        return key;
    }

    @Override
    public String getName() {
        if (isDirectory()) {
            int len = key.length();
            int pos = key.lastIndexOf('/', len - 2);
            return pos != -1 ? key.substring(pos + 1, len - 1) : key.substring(0, len - 1);
        } else {
            int pos = key.lastIndexOf("/");
            return pos != -1 ? key.substring(pos + 1) : key;
        }
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return key.endsWith("/");
    }

    @Override
    public boolean isFile() {
        return !isDirectory();
    }

    @Override
    public boolean doesExist() {
        if (exist == null) {
            try {
                this.exist = isFile()
                        ? requestObjectHead().isPresent()
                        : !client.listObjectsV2(req -> req.bucket(bucket).prefix(key)).contents().isEmpty();
            } catch (NoSuchKeyException e) {
                this.exist = false;
            }
        }

        return exist;
    }

    @Override
    public boolean isReadable() {
        return doesExist();
    }

    @Override
    public boolean isWritable() {
        return user.authorize(new WriteRequest(getPhysicalFile().toString())) != null;
    }

    @Override
    public boolean isRemovable() {
        if (key.equals("/")) {
            return false;
        }

        if (!isWritable()) {
            return false;
        }

        if (isDirectory()) {
            return listFiles().stream()
                    .allMatch(v -> v.getAbsolutePath().equals(getAbsolutePath()));
        }

        return true;
    }

    @Override
    public String getOwnerName() {
        return user.getName();
    }

    @Override
    public String getGroupName() {
        return bucket;
    }

    @Override
    public int getLinkCount() {
        return isDirectory() ? 3 : 0;
    }

    @Override
    public long getLastModified() {
        if (lastModified == null) {
            this.lastModified = requestObjectHead()
                    .map(v -> v.lastModified().getEpochSecond())
                    .orElse(null);
        }

        return lastModified == null ? -1 : lastModified;
    }

    @Override
    public boolean setLastModified(long time) {
        return false;
    }

    @Override
    public long getSize() {
        if (isDirectory()) {
            return 0;
        }

        if (size == null) {
            this.size = requestObjectHead().map(HeadObjectResponse::contentLength).orElse(null);
        }

        return size == null ? -1 : size;
    }

    @Override
    public Object getPhysicalFile() {
        return "%s/%s".formatted(bucket, key);
    }

    @Override
    public boolean mkdir() {
        String dirKey = key.endsWith("/") ? key : key + "/";

        try {
            client.putObject(
                    req -> req.bucket(bucket).key(dirKey).contentType("application/x-directory"),
                    RequestBody.empty()
            );
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public boolean delete() {
        if (!isRemovable()) {
            return false;
        }

        try {
            client.deleteObject(req -> req.bucket(bucket).key(key));
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public boolean move(FtpFile destination) {
        if (!destination.isWritable() || !isWritable()) {
            return false;
        }

        try {
            // 复制数据
            client.copyObject(req -> req
                    .sourceBucket(bucket)
                    .sourceKey(key)
                    .destinationBucket(bucket)
                    .destinationKey(destination.getAbsolutePath())
            );
            // 确认数据复制成功
            client.headObject(req -> req.bucket(bucket).key(destination.getAbsolutePath()));
            // 删除源数据
            client.deleteObject(req -> req.bucket(bucket).key(key));
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public List<? extends FtpFile> listFiles() {
        if (!isDirectory()) {
            return null;
        }

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                .bucket(bucket)
                //TODO configuration
                .maxKeys(10000)
                .delimiter("/");
        if (!key.equals("/")) {
            builder.prefix(key);
        }
        ListObjectsV2Response resp = client.listObjectsV2(builder.build());

        return Stream.concat(
                        resp.commonPrefixes().stream().map(CommonPrefix::prefix),
                        resp.contents().stream().map(S3Object::key)
                )
                .map(key -> new S3FtpFile(client, bucket, key, user))
                .toList();
    }

    @Override
    public OutputStream createOutputStream(long offset) {
        return new S3OutputStream(client, bucket, key, offset);
    }

    @Override
    public InputStream createInputStream(long offset) {
        return client.getObject(req -> req.bucket(bucket).key(key).range("%s-".formatted(offset)));
    }

    private Optional<HeadObjectResponse> requestObjectHead() {
        try {
            return Optional.of(client.headObject(req -> req.bucket(bucket).key(key)));
        } catch (AwsServiceException | SdkClientException e) {
            return Optional.empty();
        }
    }

}
