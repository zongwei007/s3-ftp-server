package com.s3.ftp.s3;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;

class S3OutputStream extends OutputStream {

    public S3OutputStream(S3Client client, String bucket, String key, long offset) {

    }

    @Override
    public void write(int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        super.write(bytes, off, len);
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
