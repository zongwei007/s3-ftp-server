package com.s3.ftp.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PathBuilderTest {

    @Test
    void test() {
        assertEquals(PathBuilder.from("").build(), "");
        assertEquals(PathBuilder.from("test")
                        .resolve("a")
                        .resolve("b")
                        .build(),
                "test/a/b"
        );
        assertEquals(PathBuilder.from("test")
                        .resolve("b")
                        .resolve("..")
                        .resolve("a")
                        .build(),
                "test/a"
        );
        assertEquals(PathBuilder.from("test/b/a/")
                        .resolve("../c/")
                        .build(),
                "test/b/c"
        );
        assertEquals(PathBuilder.from("/a/b/c/")
                        .build(),
                "a/b/c"
        );
    }

}