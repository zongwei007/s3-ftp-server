package com.s3.ftp.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

class S3OutputStream extends OutputStream {

    private final S3Client client;

    private final String bucket;

    private final String key;

    //TODO offset
    public S3OutputStream(S3Client client, String bucket, String key, @SuppressWarnings("unused") long offset) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        String contentType = Files.probeContentType(Path.of(key));

        client.putObject(
                req -> req.bucket(bucket).key(key).contentType(contentType),
                RequestBody.fromBytes(ByteBuffer.wrap(bytes, off, len).array())
        );
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }
}
