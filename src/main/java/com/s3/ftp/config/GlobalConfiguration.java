package com.s3.ftp.config;

import com.s3.ftp.s3.S3ClientRegistry;
import com.s3.ftp.s3.S3FileSystemFactory;
import org.apache.ftpserver.*;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfiguration;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class GlobalConfiguration {

    private static final String DEFAULT_LISTENER_NAME = "default";

    private static Properties configProperties;

    private static String bindAddress;

    private static int port = 2121;

    private static int idleTimeout = 300;

    private static String passiveExternalAddress;

    private static String passivePorts;

    private static String keyStorePath;

    private static String keyStorePassword;

    private static boolean implicitSsl = false;

    private static boolean anonymousEnabled = false;

    private static int maxLogins = 10;

    private static int maxThreads = 0;

    /**
     * 遍历目录时最大返回数据量，过低可能引起执行 LIST 操作时数据缺失。
     */
    public static int maxListKeysLimit = 10000;

    /**
     * 服务端向 S3 写入前的写缓存，默认为 10MiB，不得小于 5MiB
     */
    public static int writeBufferSize = 1024 * 1024 * 10;

    /**
     * 模拟追加写时，需要先将 S3 数据取回再写入。这里设置此类行为时最大支持的追加偏移量。
     */
    public static int maxAppendOffsetSize = 1024 * 1024 * 20;

    private GlobalConfiguration() {
        //private
    }

    public static FtpServerFactory createServerFactory() throws IOException {
        ListenerFactory factory = GlobalConfiguration.createListenerFactory();

        FtpServerFactory serverFactory = new FtpServerFactory();
        serverFactory.setConnectionConfig(GlobalConfiguration.createConnectionConfig());
        serverFactory.addListener(DEFAULT_LISTENER_NAME, factory.createListener());

        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(new S3ClientRegistry(configProperties));
        serverFactory.setFileSystem(fileSystemFactory);

        if (configProperties.keySet().stream().anyMatch(v -> v.toString().startsWith("ftpserver.user"))) {
            PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();

            Path userFile = createUserProperties(configProperties);
            userManagerFactory.setFile(userFile.toFile());
            serverFactory.setUserManager(userManagerFactory.createUserManager());
        }

        return serverFactory;
    }

    public static void load(Path path) throws IOException {
        if (Files.notExists(path)) {
            throw new IllegalArgumentException("Configuration not found");
        }

        init(readConfigProperties(path));
    }

    static void init(Properties props) {
        bindAddress = props.getProperty("s3-ftp.bind-address");
        port = Integer.parseInt(props.getProperty("s3-ftp.port", String.valueOf(port)));
        idleTimeout = Integer.parseInt(props.getProperty("s3-ftp.idle-timeout", String.valueOf(idleTimeout)));
        passiveExternalAddress = props.getProperty("s3-ftp.passive.external-address");
        passivePorts = props.getProperty("s3-ftp.passive.ports");
        keyStorePath = props.getProperty("s3-ftp.ssl.key-path");
        keyStorePassword = props.getProperty("s3-ftp.ssl.key-password");
        implicitSsl = Boolean.parseBoolean(props.getProperty("s3-ftp.ssl.implicit", String.valueOf(implicitSsl)));
        anonymousEnabled = Boolean.parseBoolean(
                props.getProperty("s3-ftp.anonymous-enabled", String.valueOf(anonymousEnabled))
        );
        maxLogins = Integer.parseInt(props.getProperty("s3-ftp.max-logins", String.valueOf(maxLogins)));
        maxThreads = Integer.parseInt(props.getProperty("s3-ftp.max-threads", String.valueOf(maxThreads)));

        maxListKeysLimit = Integer.parseInt(
                props.getProperty("s3-ftp.max-list-keys-limit", String.valueOf(maxListKeysLimit))
        );
        writeBufferSize = Integer.parseInt(
                props.getProperty("s3-ftp.write-buffer-size", String.valueOf(writeBufferSize))
        );
        maxAppendOffsetSize = Integer.parseInt(
                props.getProperty("s3-ftp.max-append-offset-size", String.valueOf(maxAppendOffsetSize))
        );

        if (1024 * 1024 * 5 > writeBufferSize) {
            throw new IllegalArgumentException("write-buffer-size must large then 5MiB");
        }

        GlobalConfiguration.configProperties = props;
    }

    private static Properties readConfigProperties(Path path) throws IOException {
        Properties properties = new Properties();
        try (InputStream is = Files.newInputStream(path)) {
            properties.load(is);
        }
        return properties;
    }

    private static ListenerFactory createListenerFactory() {
        ListenerFactory factory = new ListenerFactory();
        factory.setServerAddress(bindAddress);
        factory.setPort(port);
        factory.setIdleTimeout(idleTimeout);

        SslConfiguration sslConfiguration = null;
        if (keyStorePath != null && keyStorePassword != null) {
            SslConfigurationFactory ssl = new SslConfigurationFactory();
            ssl.setKeystoreFile(new File(keyStorePath));
            ssl.setKeystorePassword(keyStorePassword);
            sslConfiguration = ssl.createSslConfiguration();
        }

        factory.setDataConnectionConfiguration(createDataConnectionConfiguration(sslConfiguration));
        factory.setSslConfiguration(sslConfiguration);
        factory.setImplicitSsl(implicitSsl);

        return factory;
    }

    private static DataConnectionConfiguration createDataConnectionConfiguration(SslConfiguration ssl) {
        DataConnectionConfigurationFactory factory = new DataConnectionConfigurationFactory();
        factory.setSslConfiguration(ssl);
        factory.setImplicitSsl(implicitSsl);
        factory.setIdleTime(idleTimeout);
        factory.setPassiveExternalAddress(passiveExternalAddress);
        if (passivePorts != null) {
            factory.setPassivePorts(passivePorts);
        }

        return factory.createDataConnectionConfiguration();
    }

    private static ConnectionConfig createConnectionConfig() {
        ConnectionConfigFactory factory = new ConnectionConfigFactory();
        factory.setAnonymousLoginEnabled(anonymousEnabled);
        factory.setMaxLogins(maxLogins);
        factory.setMaxThreads(maxThreads);
        return factory.createConnectionConfig();
    }

    private static Path createUserProperties(Properties configProperties) throws IOException {
        Path userFile = Files.createTempFile("s3-ftp-user", ".properties");
        try (OutputStream os = Files.newOutputStream(userFile)) {
            configProperties.store(os, "Users");
        }
        return userFile;
    }
}
