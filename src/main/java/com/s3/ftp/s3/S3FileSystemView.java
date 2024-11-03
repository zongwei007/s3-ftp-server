package com.s3.ftp.s3;

import com.s3.ftp.util.PathBuilder;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

public final class S3FileSystemView implements FileSystemView {

    private final S3Client client;

    private final String bucket;

    private final String rootPath;

    private final User user;

    private String currentPath = "/";

    public S3FileSystemView(S3Client client, String bucket, String path, User user) {
        this.client = client;
        this.bucket = bucket;
        this.rootPath = PathBuilder.from(path).buildDirPath();
        this.user = user;
    }

    @Override
    public FtpFile getHomeDirectory() {
        return new S3FtpFile(client, bucket, rootPath, user);
    }

    @Override
    public FtpFile getWorkingDirectory() {
        String workingPath = PathBuilder.from(rootPath).resolve(currentPath).buildDirPath();
        return new S3FtpFile(client, bucket, workingPath, user);
    }

    @Override
    public boolean changeWorkingDirectory(String dir) {
        PathBuilder pathBuilder = PathBuilder.from(rootPath);
        if (!dir.isEmpty() && dir.charAt(0) != '/') {
            pathBuilder.resolve(currentPath);
        }

        String workingPath = pathBuilder.resolve(dir).buildDirPath();
        if (workingPath.equals(rootPath)) {
            this.currentPath = dir;
            return true;
        }

        try {
            List<S3Object> contents = client.listObjectsV2(req -> req.bucket(bucket).prefix(workingPath))
                    .contents();
            if (!contents.isEmpty()) {
                this.currentPath = workingPath;
                return true;
            }

            return false;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public FtpFile getFile(String path) {
        String filePath = PathBuilder.from(rootPath).resolve(currentPath).resolve(path).build();
        return new S3FtpFile(client, bucket, filePath, user);
    }

    @Override
    public boolean isRandomAccessible() {
        return true;
    }

    @Override
    public void dispose() {
        client.close();
    }


}