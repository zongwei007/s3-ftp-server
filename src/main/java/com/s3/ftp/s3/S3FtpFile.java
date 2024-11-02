package com.s3.ftp.s3;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

public class S3FtpFile implements FtpFile {

    private final S3Client client;

    private final String bucket;

    private final String key;

    private final User user;

    private Boolean exist;

    private Long lastModified;

    private Long size;

    private String owner;

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
        int pos = key.lastIndexOf("/");
        return pos != -1 ? key.substring(pos + 1) : key;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isFile() {
        return true;
    }

    @Override
    public boolean doesExist() {
        if (exist == null) {
            try {
                this.exist = getFileHead().isPresent();
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
        return true;
    }

    @Override
    public boolean isRemovable() {
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
            this.lastModified = getFileHead()
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
        if (size == null) {
            this.size = getFileHead().map(HeadObjectResponse::contentLength).orElse(null);
        }

        return size == null ? -1 : size;
    }

    @Override
    public Object getPhysicalFile() {
        return "%s/%s".formatted(bucket, key);
    }

    @Override
    public boolean mkdir() {
        try {
            client.putObject(req -> req.bucket(bucket).key(key + "/"), RequestBody.empty());
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public boolean delete() {
        try {
            client.deleteObject(req -> req.bucket(bucket).key(key));
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public boolean move(FtpFile destination) {
        try {
            client.copyObject(req -> req
                    .sourceBucket(bucket)
                    .sourceKey(key)
                    .destinationBucket(bucket)
                    .destinationKey(destination.getAbsolutePath())
            );
            client.deleteObject(req -> req.bucket(bucket).key(key));
            return true;
        } catch (AwsServiceException | SdkClientException e) {
            return false;
        }
    }

    @Override
    public List<? extends FtpFile> listFiles() {
        return client.listObjectsV2(req -> req.bucket(bucket).prefix(key + "/")).contents()
                .stream()
                .map(S3Object::key)
                .map(key -> new S3FtpFile(client, bucket, key, user))
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

    private Optional<HeadObjectResponse> getFileHead() {
        try {
            return Optional.of(client.headObject(req -> req.bucket(bucket).key(key)));
        } catch (AwsServiceException | SdkClientException e) {
            return Optional.empty();
        }
    }

}
