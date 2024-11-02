package com.s3.ftp.s3;

import com.robothy.s3.rest.LocalS3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class S3ClientRegistryTest {

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
    void byKey() {
        S3ClientRegistry registry = buildRegistry();
        S3ClientBuilder builder = registry.byKey("test");

        assertNotNull(builder);

        try (S3Client client = builder.build()) {
            client.createBucket(req -> req.bucket("test"));
        }
    }

    private S3ClientRegistry buildRegistry() {
        Properties properties = new Properties();
        properties.put("s3.test.uri", "http://127.0.0.1:%s".formatted(localS3.getPort()));
        properties.put("s3.test.access_key", "foo");
        properties.put("s3.test.secret_key", "bar");

        return new S3ClientRegistry(properties);
    }
}