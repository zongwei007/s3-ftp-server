package com.s3.ftp.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UserStoreInfoTest {

    @Test
    void fromHomeDirectory() {
        UserStoreInfo info = UserStoreInfo.fromHomeDirectory("test:demo/123");

        assertEquals(info.store(), "test");
        assertEquals(info.bucket(), "demo");
        assertEquals(info.path(), "123");
    }

    @Test
    void fromNoPath() {
        UserStoreInfo info = UserStoreInfo.fromHomeDirectory("test:demo");

        assertEquals(info.store(), "test");
        assertEquals(info.bucket(), "demo");
        assertEquals(info.path(), "");
    }
}