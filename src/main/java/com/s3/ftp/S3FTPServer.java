package com.s3.ftp;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.filesystem.nativefs.NativeFileSystemFactory;
import org.apache.ftpserver.impl.DefaultConnectionConfig;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class S3FTPServer {

    private static final String DEFAULT_LISTENER_NAME = "default";

    public static void main(String[] args) throws Exception {
        Path path = Path.of("./users.properties");
        if (Files.notExists(path)) {
            Files.createFile(path);
        }

        DefaultConnectionConfig connectionConfig = new DefaultConnectionConfig();

        ListenerFactory factory = new ListenerFactory();
        factory.setPort(2121);

        FtpServerFactory serverFactory = new FtpServerFactory();
        serverFactory.setConnectionConfig(connectionConfig);
        serverFactory.addListener(DEFAULT_LISTENER_NAME, factory.createListener());

        serverFactory.setFileSystem(new NativeFileSystemFactory());

        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setFile(new File("./users.properties"));

        serverFactory.setUserManager(userManagerFactory.createUserManager());


//        serverFactory.setFileSystem(new S3FileSystemFactory());
//        SslConfigurationFactory ssl = new SslConfigurationFactory();
//        ssl.setKeystoreFile(new File("src/test/resources/ftpserver.jks"));
//        ssl.setKeystorePassword("password");
//        factory.setSslConfiguration(ssl.createSslConfiguration());


        FtpServer server = serverFactory.createServer();
        server.start();
    }

}
