package com.s3.ftp.s3;

import com.s3.ftp.jupiter.LocalS3;
import org.apache.commons.lang3.RandomUtils;
import org.apache.ftpserver.ftplet.AuthorizationRequest;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.WriteRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("JUnitMalformedDeclaration")
@LocalS3
class S3FtpFileTest {

    private static final String BUCKET = "test";

    @BeforeAll
    static void beforeAll(S3Client client) {
        client.createBucket(req -> req.bucket(BUCKET));
        client.putObject(req -> req.bucket(BUCKET).key("0.txt"), RequestBody.fromString("0"));
        client.putObject(req -> req.bucket(BUCKET).key("a/1.txt"), RequestBody.fromString("1"));
        client.putObject(req -> req.bucket(BUCKET).key("a/b/2.txt"), RequestBody.fromString("2"));
        client.putObject(req -> req.bucket(BUCKET).key("a/b/3.txt"), RequestBody.fromString("3"));
        client.putObject(req -> req.bucket(BUCKET).key("b/"), RequestBody.empty());
    }

    @Test
    void testGetter(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");

        S3FtpFile file = new S3FtpFile(client, BUCKET, "a/1.txt", user);

        assertEquals("a/1.txt", file.getAbsolutePath());
        assertEquals("1.txt", file.getName());
        assertFalse(file.isHidden());
        assertFalse(file.isDirectory());
        assertTrue(file.isFile());
        assertTrue(file.doesExist());
        assertEquals("user", file.getOwnerName());
        assertEquals(BUCKET, file.getGroupName());
        assertTrue(file.getLastModified() > -1);
        assertEquals(1, file.getSize());
        assertEquals(BUCKET + "/a/1.txt", file.getPhysicalFile());

        S3FtpFile dir = new S3FtpFile(client, BUCKET, "a/", user);
        assertTrue(dir.doesExist());
        assertTrue(dir.isDirectory());
        assertEquals(0, dir.getSize());
        assertEquals("a", dir.getName());

        dir = new S3FtpFile(client, BUCKET, "b/", user);
        assertTrue(dir.doesExist());

        dir = new S3FtpFile(client, BUCKET, "a/b/", user);
        assertEquals("b", dir.getName());
    }

    @Test
    void testPermissions(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(Mockito.any(WriteRequest.class))).thenAnswer(iom -> {
            AuthorizationRequest argument = iom.getArgument(0);
            if (argument instanceof WriteRequest wa && wa.getFile().startsWith(BUCKET + "/a/")) {
                return wa;
            }
            return null;
        });

        S3FtpFile file = new S3FtpFile(client, BUCKET, "a/1.txt", user);
        assertTrue(file.isReadable());
        assertTrue(file.isWritable());
        assertTrue(file.isRemovable());

        file = new S3FtpFile(client, BUCKET, "a/2.txt", user);
        assertFalse(file.isReadable());
        assertTrue(file.isWritable());
        assertTrue(file.isRemovable());

        file = new S3FtpFile(client, BUCKET, "a/", user);
        assertTrue(file.isReadable());
        assertTrue(file.isWritable());
        assertFalse(file.isRemovable());

        file = new S3FtpFile(client, BUCKET, "c/", user);
        assertFalse(file.isReadable());
        assertFalse(file.isWritable());
        assertFalse(file.isRemovable());

        file = new S3FtpFile(client, BUCKET, "/", user);
        assertFalse(file.isRemovable());
    }

    @Test
    void testMkdir(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(any(AuthorizationRequest.class))).thenAnswer(iom -> iom.getArgument(0));

        S3FtpFile file = new S3FtpFile(client, BUCKET, "c/", user);
        assertTrue(file.mkdir());
        assertNotNull(client.headObject(req -> req.bucket(BUCKET).key("c/")));
        assertTrue(file.delete());
    }

    @Test
    void testDelete(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(any(AuthorizationRequest.class))).thenAnswer(iom -> iom.getArgument(0));

        client.putObject(req -> req.bucket(BUCKET).key("a/0.txt"), RequestBody.fromString("0"));

        S3FtpFile file = new S3FtpFile(client, BUCKET, "a/0.txt", user);
        assertTrue(file.delete());
        assertThrows(NoSuchKeyException.class, () -> client.headObject(req -> req.bucket(BUCKET).key("a/0.txt")));
        assertTrue(file.delete());
    }

    @Test
    void testMove(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(any(AuthorizationRequest.class))).thenAnswer(iom -> iom.getArgument(0));

        client.putObject(req -> req.bucket(BUCKET).key("a/0.txt"), RequestBody.fromString("0"));

        S3FtpFile src = new S3FtpFile(client, BUCKET, "a/0.txt", user);
        S3FtpFile dst = new S3FtpFile(client, BUCKET, "c/0.txt", user);

        assertTrue(src.move(dst));
        assertNotNull(client.headObject(req -> req.bucket(BUCKET).key("c/0.txt")));
        assertThrows(NoSuchKeyException.class, () -> client.headObject(req -> req.bucket(BUCKET).key("a/0.txt")));
        assertTrue(dst.delete());
    }

    @Test
    void testListFiles(S3Client client) {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");

        List<? extends FtpFile> files = new S3FtpFile(client, BUCKET, "/", user).listFiles();
        assertNotNull(files);
        assertEquals(3, files.size());
        assertArrayEquals(new String[]{"a", "b", "0.txt"}, files.stream().map(FtpFile::getName).toArray(String[]::new));

        files = new S3FtpFile(client, BUCKET, "a/", user).listFiles();
        assertNotNull(files);
// TODO
//        assertEquals(2, files.size());
//        assertArrayEquals(new String[]{"b", "1.txt"}, files.stream().map(FtpFile::getName).toArray(String[]::new));
    }

    @Test
    void testCreateInputStream(S3Client client) throws IOException {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(any(AuthorizationRequest.class))).thenAnswer(iom -> iom.getArgument(0));

        byte[] small = RandomUtils.nextBytes(1024 * 100);
        client.putObject(req -> req.bucket(BUCKET).key("small.dat"), RequestBody.fromBytes(small));

        S3FtpFile file = new S3FtpFile(client, BUCKET, "small.dat", user);
        try (InputStream is = file.createInputStream(0)) {
            byte[] result = is.readAllBytes();
            assertArrayEquals(small, result);
        } finally {
            assertTrue(file.delete());
        }

// TODO
//        try (InputStream is = file.createInputStream(1024 * 50); ByteArrayOutputStream os = new ByteArrayOutputStream()) {
//            is.transferTo(os);
//            byte[] result = os.toByteArray();
//            assertArrayEquals(Arrays.copyOfRange(small, 1024 * 50, 1024 * 100), result);
//        } finally {
//            assertTrue(file.delete());
//        }
    }

    @Test
    void testCreateOutputStream(S3Client client) throws IOException {
        User user = mock(User.class);
        when(user.getName()).thenReturn("user");
        when(user.authorize(any(AuthorizationRequest.class))).thenAnswer(iom -> iom.getArgument(0));

        byte[] small = RandomUtils.nextBytes(1024 * 100);
        S3FtpFile file = new S3FtpFile(client, BUCKET, "small.dat", user);
        try (OutputStream os = file.createOutputStream(0); InputStream is = new ByteArrayInputStream(small)) {
            is.transferTo(os);
        }

        byte[] result = client.getObject(req -> req.bucket(BUCKET).key("small.dat"), ResponseTransformer.toBytes())
                .asByteArray();
        assertArrayEquals(small, result);
    }
}