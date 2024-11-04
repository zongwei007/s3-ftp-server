package com.s3.ftp.s3;

import com.s3.ftp.config.GlobalConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class S3OutputStream extends OutputStream {

    private static final int MULTIPART_SIZE_MIN = 1024 * 1024 * 5;

    private final S3Client client;

    private final String bucket;

    private final String key;

    private final String contentType;

    private final ByteBuffer writeBuffer;

    private long offset;

    private String uploadId;

    private int uploadPartNumber = 0;

    private Map<Integer, String> completedParts;

    private boolean closed = false;

    public S3OutputStream(S3Client client, String bucket, String key, long offset) throws IOException {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.contentType = Files.probeContentType(Path.of(key));
        this.writeBuffer = ByteBuffer.allocate(GlobalConfiguration.writeBufferSize);
        this.offset = offset;
        if (offset > GlobalConfiguration.maxAppendOffsetSize) {
            throw new IOException("offset over limit: %s byte".formatted(GlobalConfiguration.maxAppendOffsetSize));
        }
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b});
    }

    @Override
    public synchronized void write(byte[] bytes, int off, int len) throws IOException {
        if (offset != 0) {
            this.offset = 0;

            try (InputStream is = client.getObject(req -> req.bucket(bucket).key(key))) {
                is.transferTo(this);
            } catch (NoSuchKeyException e) {
                throw new IOException("file %s not found".formatted(key), e);
            }
        }

        try {
            if (writeBuffer.remaining() >= len - off) {
                writeBuffer.put(bytes, off, len);
            } else {
                int remaining = writeBuffer.remaining();
                writeBuffer.put(bytes, off, remaining);
                appendMultipartObject();
                writeBuffer.rewind();
                writeBuffer.put(bytes, off + remaining, len - remaining);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }

            this.closed = true;
        }

        if (uploadId == null) {
            putObjectSingle();
            return;
        }

        appendMultipartObject();
        completeMultipartUpload();
    }

    @Override
    public synchronized void flush() {
        if (writeBuffer.position() > MULTIPART_SIZE_MIN) {
            appendMultipartObject();
            writeBuffer.rewind();
        }
    }

    private void putObjectSingle() {
        if (writeBuffer.position() == 0) {
            return;
        }

        writeBuffer.flip();
        client.putObject(
                req -> req.bucket(bucket).key(key).contentType(contentType),
                RequestBody.fromByteBuffer(writeBuffer)
        );
    }

    private void appendMultipartObject() {
        if (uploadId == null) {
            CreateMultipartUploadResponse cResp = client.createMultipartUpload(
                    req -> req.bucket(bucket).key(key).contentType(contentType)
            );

            this.uploadId = cResp.uploadId();
            this.completedParts = new HashMap<>();
        }

        writeBuffer.flip();
        this.uploadPartNumber += 1;
        UploadPartResponse pResp = client.uploadPart(
                req -> req.bucket(bucket).key(key).uploadId(uploadId).partNumber(uploadPartNumber),
                RequestBody.fromByteBuffer(writeBuffer)
        );
        completedParts.put(uploadPartNumber, pResp.eTag());
    }

    private void completeMultipartUpload() {
        List<CompletedPart> parts = completedParts.entrySet().stream()
                .map(ele -> CompletedPart.builder().partNumber(ele.getKey()).eTag(ele.getValue()).build())
                .toList();

        client.completeMultipartUpload(req -> req.bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(mu -> mu.parts(parts))
        );
    }
}
