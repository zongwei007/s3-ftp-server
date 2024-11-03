package com.s3.ftp.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class S3OutputStream extends OutputStream {

    private final S3Client client;

    private final String bucket;

    private final String key;

    private final String contentType;

    private final ByteBuffer writeBuffer;

    private String uploadId;

    private int uploadPartNumber = 0;

    private List<CompletedPart> completedParts;

    //TODO offset
    public S3OutputStream(S3Client client, String bucket, String key, @SuppressWarnings("unused") long offset) throws IOException {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.contentType = Files.probeContentType(Path.of(key));
        //TODO configuration
        this.writeBuffer = ByteBuffer.allocate(1024 * 1024 * 10);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b});
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        try {
            int size = len - off;
            if (writeBuffer.remaining() >= size) {
                writeBuffer.put(bytes, off, len);
            } else {
                int remaining = writeBuffer.remaining();
                writeBuffer.put(bytes, off, remaining);
                appendMultipartObject();
                writeBuffer.rewind();
                writeBuffer.put(bytes, off + remaining, len - remaining);
            }

            if (writeBuffer.remaining() == 0) {
                appendMultipartObject();
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        if (uploadId == null) {
            putObjectSingle();
        } else {
            appendMultipartObject();
        }
        completeMultipartUpload();
    }

    @Override
    public void flush() {
        if (uploadId != null) {
            appendMultipartObject();
        }
    }

    private void putObjectSingle() {
        writeBuffer.flip();

        client.putObject(
                req -> req.bucket(bucket).key(key).contentType(contentType),
                RequestBody.fromByteBuffer(writeBuffer)
        );
    }

    private void appendMultipartObject() {
        if (uploadId == null) {
            CreateMultipartUploadResponse resp = client.createMultipartUpload(
                    req -> req.bucket(bucket).key(key).contentType(contentType)
            );

            this.uploadId = resp.uploadId();
            this.completedParts = new ArrayList<>();
        }

        if (writeBuffer.position() == 0) {
            return;
        }

        writeBuffer.flip();
        this.uploadPartNumber += 1;
        client.uploadPart(
                req -> req.bucket(bucket).key(key).uploadId(uploadId).partNumber(uploadPartNumber),
                RequestBody.fromByteBuffer(writeBuffer)
        );
        completedParts.add(CompletedPart.builder().partNumber(uploadPartNumber).build());
    }

    private void completeMultipartUpload() {
        if (uploadId != null && !completedParts.isEmpty()) {
            client.completeMultipartUpload(req -> req.bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(mu -> mu.parts(completedParts))
            );
        }
    }
}
