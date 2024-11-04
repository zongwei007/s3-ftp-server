package com.s3.ftp.s3;

import com.robothy.s3.rest.LocalS3;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3FileSystemFactoryTest {

    static LocalS3 localS3 = LocalS3.builder().port(-1).build();

    @BeforeAll
    static void beforeAll() {
        localS3.start();
    }

    @AfterAll
    static void afterAll() {
        localS3.shutdown();
    }

    @Test
    void testCreateFileSystemView() throws Exception {
        User user = mock(User.class);
        when(user.getHomeDirectory()).thenReturn("test:test");

        S3ClientRegistry registry = mock(S3ClientRegistry.class);
        when(registry.byKey(Mockito.eq("test"))).thenReturn(S3Client.builder()
                .endpointOverride(URI.create("http://127.0.0.1:%s".formatted(localS3.getPort())))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foo", "bar"))
                ));


        FileSystemFactory factory = new S3FileSystemFactory(registry);
        FileSystemView fileSystemView = factory.createFileSystemView(user);

        try {
            assertNotNull(fileSystemView);
        } finally {
            fileSystemView.dispose();
        }

        when(user.getHomeDirectory()).thenReturn("fake:test");
        assertThrows(FtpException.class, () -> factory.createFileSystemView(user));
    }

}