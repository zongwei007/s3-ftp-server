package com.s3.ftp.s3;

import com.s3.ftp.util.PathBuilder;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import software.amazon.awssdk.services.s3.S3Client;
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
        this.rootPath = PathBuilder.root(path).buildDirPath();
        this.user = user;
    }

    @Override
    public FtpFile getHomeDirectory() {
        return new S3FtpFile(client, bucket, rootPath, user);
    }

    @Override
    public FtpFile getWorkingDirectory() {
        String workingPath = PathBuilder.root(rootPath).resolve(currentPath).buildDirPath();
        return new S3FtpFile(client, bucket, workingPath, user);
    }

    @Override
    public boolean changeWorkingDirectory(String dir) {
        String workingPath = PathBuilder.root(rootPath).resolve(currentPath).resolve(dir).buildDirPath();
        if (rootPath.equals(workingPath)) {
            this.currentPath = rootPath;
            return true;
        }

        List<S3Object> contents = client.listObjectsV2(req -> req.bucket(bucket).prefix(workingPath).maxKeys(1))
                .contents();
        if (!contents.isEmpty()) {
            this.currentPath = workingPath;
            return true;
        }

        return false;
    }

    @Override
    public FtpFile getFile(String path) {
        PathBuilder builder = PathBuilder.root(rootPath).resolve(currentPath).resolve(path);
        String filePath = path.endsWith("/") ? builder.buildDirPath() : builder.build();
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
