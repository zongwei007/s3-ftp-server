package com.s3.ftp.s3;

import com.s3.ftp.config.GlobalConfiguration;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public final class S3FtpFile implements FtpFile {

    private static final Instant LAST_MODIFIED_EMPTY = Instant.ofEpochSecond(0);

    private final S3Client client;

    private final String bucket;

    private final String key;

    private final Instant lastModified;

    private final long size;

    private final User user;

    private Boolean exist;

    public S3FtpFile(S3Client client, String bucket, String key, User user) {
        this(client, bucket, key, LAST_MODIFIED_EMPTY, 0L, user);
    }

    public S3FtpFile(S3Client client, String bucket, String key, Instant lastModified, Long size, User user) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.lastModified = lastModified;
        this.size = size;
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
            this.exist = !client.listObjectsV2(req -> req.bucket(bucket).prefix(key).maxKeys(1))
                    .contents()
                    .isEmpty();
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
            return Optional.ofNullable(listFiles())
                    .map(Collection::stream)
                    .orElse(Stream.of())
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
        return lastModified.toEpochMilli();
    }

    @Override
    public boolean setLastModified(long time) {
        return false;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Object getPhysicalFile() {
        return "/%s/%s".formatted(bucket, key);
    }

    @Override
    public boolean mkdir() {
        if (!isDirectory()) {
            return false;
        }

        client.putObject(
                req -> req.bucket(bucket).key(key).contentType("application/x-directory"),
                RequestBody.empty()
        );
        return true;
    }

    @Override
    public boolean delete() {
        if (!isRemovable()) {
            return false;
        }

        client.deleteObject(req -> req.bucket(bucket).key(key));
        return true;
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
                .maxKeys(GlobalConfiguration.maxListKeysLimit)
                .delimiter("/");

        if (!key.equals("/")) {
            builder.prefix(key);
        }
        ListObjectsV2Response resp = client.listObjectsV2(builder.build());

        return Stream.concat(
                        resp.commonPrefixes().stream()
                                .map(v -> new S3FtpFile(client, bucket, v.prefix(), user)),
                        resp.contents().stream()
                                .map(v -> new S3FtpFile(client, bucket, v.key(), v.lastModified(), v.size(), user))
                )
                .toList();
    }

    @Override
    public OutputStream createOutputStream(long offset) throws IOException {
        return new S3OutputStream(client, bucket, key, offset);
    }

    @Override
    public InputStream createInputStream(long offset) {
        return client.getObject(req -> req.bucket(bucket).key(key).range("%s-".formatted(offset)));
    }

}
