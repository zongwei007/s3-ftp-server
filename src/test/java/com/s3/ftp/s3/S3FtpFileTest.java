package com.s3.ftp.s3;

import com.s3.ftp.jupiter.LocalS3;
import org.apache.ftpserver.ftplet.User;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("JUnitMalformedDeclaration")
@LocalS3
class S3FtpFileTest {

    private static final String BUCKET = "test";

    @BeforeAll
    static void beforeAll(S3Client client) {
        client.createBucket(req -> req.bucket(BUCKET));
    }

    @Test
    void testGetter(S3Client client) throws Exception {
        User user = Mockito.mock(User.class);
        Mockito.when(user.getName()).thenReturn("user");

        client.putObject(req -> req.bucket(BUCKET).key("a/1.txt"), RequestBody.fromString("1"));

        S3FtpFile file = new S3FtpFile(client, BUCKET, "a/1.txt", user);

        assertEquals("a/1.txt", file.getAbsolutePath());
        assertEquals("1.txt", file.getName());
        assertTrue(file.getLastModified() > -1);
        assertEquals("user", file.getOwnerName());
    }

}