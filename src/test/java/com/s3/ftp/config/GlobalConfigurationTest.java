package com.s3.ftp.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobalConfigurationTest {

    @Test
    void testInit() {
        Properties properties = new Properties();
        properties.setProperty("s3-ftp.max-list-keys-limit", "1000");
        properties.setProperty("s3-ftp.write-buffer-size", "100000000");
        properties.setProperty("s3-ftp.max-append-offset-size", "100000000");

        GlobalConfiguration.init(properties);

        assertEquals(1000, GlobalConfiguration.maxListKeysLimit);
        assertEquals(100000000, GlobalConfiguration.writeBufferSize);
        assertEquals(100000000, GlobalConfiguration.maxAppendOffsetSize);
    }
}