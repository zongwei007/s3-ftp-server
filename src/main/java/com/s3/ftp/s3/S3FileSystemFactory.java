package com.s3.ftp.s3;

import com.s3.ftp.config.UserStoreInfo;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public final class S3FileSystemFactory implements FileSystemFactory {

    private final S3ClientRegistry clientRegister;

    public S3FileSystemFactory(S3ClientRegistry clientRegister) {
        this.clientRegister = clientRegister;
    }

    @Override
    public FileSystemView createFileSystemView(User user) throws FtpException {
        UserStoreInfo storeInfo = UserStoreInfo.fromHomeDirectory(user.getHomeDirectory());

        S3ClientBuilder builder = clientRegister.byKey(storeInfo.store());
        if (builder == null) {
            throw new FtpException("Home directory %s not found".formatted(user.getHomeDirectory()));
        }

        return new S3FileSystemView(builder.build(), storeInfo.bucket(), storeInfo.path(), user);
    }

}
