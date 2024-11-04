package com.s3.ftp.s3;

import com.s3.ftp.jupiter.LocalS3;
import org.apache.ftpserver.ftplet.User;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("JUnitMalformedDeclaration")
@LocalS3
class S3FileSystemViewTest {

    private static final String BUCKET = "test";

    static User user = Mockito.mock(User.class);

    @BeforeAll
    static void beforeAll(S3Client client) {
        client.createBucket(req -> req.bucket(BUCKET));
    }

    @Test
    void testGetHomeDirectory(S3Client client) {
        client.putObject(req -> req.bucket(BUCKET).key("a/b/1"), RequestBody.fromString("1"));

        S3FileSystemView systemView = new S3FileSystemView(client, BUCKET, "", user);

        assertNotNull(systemView.getHomeDirectory());
        assertEquals("/", systemView.getHomeDirectory().getAbsolutePath());

        systemView.changeWorkingDirectory("a/b");
        assertEquals("/", systemView.getHomeDirectory().getAbsolutePath());

        systemView = new S3FileSystemView(client, BUCKET, "a", user);

        assertNotNull(systemView.getHomeDirectory());
        assertEquals("a/", systemView.getHomeDirectory().getAbsolutePath());

        systemView.changeWorkingDirectory("b");
        assertEquals("a/", systemView.getHomeDirectory().getAbsolutePath());
    }

    @Test
    void testChangeWorkingDirectory(S3Client client) {
        client.putObject(req -> req.bucket(BUCKET).key("1.txt"), RequestBody.fromString("1"));
        client.putObject(req -> req.bucket(BUCKET).key("a/2.txt"), RequestBody.fromString("2"));
        client.putObject(req -> req.bucket(BUCKET).key("a/3"), RequestBody.fromString("3"));
        client.putObject(req -> req.bucket(BUCKET).key("a/b/4"), RequestBody.fromString("4"));
        client.putObject(req -> req.bucket(BUCKET).key("a/b/c/"), RequestBody.empty());

        S3FileSystemView systemView = new S3FileSystemView(client, BUCKET, "", user);

        assertTrue(systemView.changeWorkingDirectory("/"));
        assertTrue(systemView.changeWorkingDirectory("/a"));
        assertTrue(systemView.changeWorkingDirectory("/a/b/c"));
        assertFalse(systemView.changeWorkingDirectory("/b"));
        assertFalse(systemView.changeWorkingDirectory("/a/3"));

        systemView.changeWorkingDirectory("/a");
        assertTrue(systemView.changeWorkingDirectory("b"));
        assertTrue(systemView.changeWorkingDirectory(".."));

        systemView = new S3FileSystemView(client, BUCKET, "a", user);
        assertTrue(systemView.changeWorkingDirectory("/"));
        assertFalse(systemView.changeWorkingDirectory("/a"));
        assertTrue(systemView.changeWorkingDirectory("/b"));
    }

    @Test
    void testGetWorkingDirectory(S3Client client) {
        client.putObject(req -> req.bucket(BUCKET).key("a/b/1"), RequestBody.fromString("1"));

        S3FileSystemView systemView = new S3FileSystemView(client, BUCKET, "", user);

        assertNotNull(systemView.getWorkingDirectory());
        assertEquals("/", systemView.getWorkingDirectory().getAbsolutePath());

        systemView.changeWorkingDirectory("/a/b");
        assertEquals("a/b/", systemView.getWorkingDirectory().getAbsolutePath());

        systemView.changeWorkingDirectory("/a");
        assertEquals("a/", systemView.getWorkingDirectory().getAbsolutePath());

        systemView.changeWorkingDirectory("b");
        assertEquals("a/b/", systemView.getWorkingDirectory().getAbsolutePath());
    }

    @Test
    void testGetFile(S3Client client) {
        client.putObject(req -> req.bucket(BUCKET).key("a/b/1"), RequestBody.fromString("1"));

        S3FileSystemView systemView = new S3FileSystemView(client, BUCKET, "", user);

        assertNotNull(systemView.getFile("a/b/1"));
        assertEquals("a/b/1", systemView.getFile("a/b/1").getAbsolutePath());
        assertEquals("a/b/2", systemView.getFile("a/b/2").getAbsolutePath());

        assertNotNull(systemView.getFile("a/b/"));
        assertTrue(systemView.getFile("a/b/").isDirectory());

        systemView.changeWorkingDirectory("a/b");
        assertNotNull(systemView.getFile("1"));
        assertEquals("a/b/1", systemView.getFile("1").getAbsolutePath());
        assertEquals("a/b/2", systemView.getFile("2").getAbsolutePath());
    }

    @Test
    void testDispose(S3Client client) {
        S3FileSystemView systemView = new S3FileSystemView(client, BUCKET, "", user);
        systemView.dispose();
    }

    @Test
    void testIsRandomAccessible(S3Client client) {
        assertTrue(new S3FileSystemView(client, BUCKET, "", user).isRandomAccessible());
    }
}