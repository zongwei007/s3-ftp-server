package com.s3.ftp.s3;

import com.robothy.s3.rest.bootstrap.LocalS3Mode;
import com.s3.ftp.jupiter.LocalS3;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("JUnitMalformedDeclaration")
@LocalS3(mode = LocalS3Mode.PERSISTENCE, dataPath = "./target/s3")
class S3OutputStreamTest {

    private static final String BUCKET = "test";

    @BeforeAll
    static void beforeAll(S3Client client) {
        client.createBucket(req -> req.bucket(BUCKET));
    }

    @AfterAll
    static void afterAll(S3Client client) {
        client.deleteBucket(req -> req.bucket(BUCKET));
    }

    @Test
    void testSmallWrite(S3Client client) throws IOException {
        byte[] src = RandomUtils.nextBytes(1024 * 512);

        try (S3OutputStream os = new S3OutputStream(client, BUCKET, "small.dat", 0)) {
            os.write(src);
        }

        try (InputStream is = client.getObject(req -> req.bucket(BUCKET).key("small.dat"))) {
            assertArrayEquals(src, is.readAllBytes());
        } finally {
            client.deleteObject(req -> req.bucket(BUCKET).key("small.dat"));
        }
    }

    @Test
    void testBigWrite(S3Client client) throws IOException {
        Path tmpDir = Files.createTempDirectory("s3-ftp-os");
        Path src = tmpDir.resolve("big.dat");
        for (int i = 0; i < 10; i++) {
            byte[] bytes = RandomUtils.nextBytes(1024 * 1024 * 10);
            Files.write(src, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        Files.write(src, RandomUtils.nextBytes(5120), StandardOpenOption.APPEND);

        try (S3OutputStream os = new S3OutputStream(client, BUCKET, "big.dat", 0)) {
            Files.copy(src, os);
        }

        Path tmpOut = tmpDir.resolve("result.dat");
        client.getObject(req -> req.bucket(BUCKET).key("big.dat"), tmpOut);
        client.deleteObject(req -> req.bucket(BUCKET).key("big.dat"));

        assertTrue(PathUtils.fileContentEquals(src, tmpOut));
    }

}