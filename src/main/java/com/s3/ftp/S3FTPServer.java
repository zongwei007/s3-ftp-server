package com.s3.ftp;

import com.s3.ftp.s3.S3ClientRegistry;
import com.s3.ftp.s3.S3FileSystemFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.impl.DefaultConnectionConfig;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class S3FTPServer {

    private static final String DEFAULT_LISTENER_NAME = "default";

    public static void main(String[] args) throws Exception {
        Path path = Path.of("./config.properties");
        if (Files.notExists(path)) {
            Files.createFile(path);
        }

        DefaultConnectionConfig connectionConfig = new DefaultConnectionConfig();

        ListenerFactory factory = new ListenerFactory();
        factory.setPort(2121);

        FtpServerFactory serverFactory = new FtpServerFactory();
        serverFactory.setConnectionConfig(connectionConfig);
        serverFactory.addListener(DEFAULT_LISTENER_NAME, factory.createListener());

        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(new S3ClientRegistry(readConfigProperties(path)));
        serverFactory.setFileSystem(fileSystemFactory);

        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setFile(new File("./config.properties"));

        serverFactory.setUserManager(userManagerFactory.createUserManager());


//        serverFactory.setFileSystem(new S3FileSystemFactory());
//        SslConfigurationFactory ssl = new SslConfigurationFactory();
//        ssl.setKeystoreFile(new File("src/test/resources/ftpserver.jks"));
//        ssl.setKeystorePassword("password");
//        factory.setSslConfiguration(ssl.createSslConfiguration());



        FtpServer server = serverFactory.createServer();
        server.start();
    }

    private static Properties readConfigProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try(InputStream is = Files.newInputStream(path)) {
            properties.load(is);
        }
        return properties;
    }

}
