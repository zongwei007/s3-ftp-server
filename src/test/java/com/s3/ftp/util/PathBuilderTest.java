package com.s3.ftp.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PathBuilderTest {

    @Test
    void test() {
        assertEquals("", PathBuilder.root().build());
        assertEquals("test/a/b", PathBuilder.root("test").resolve("a").resolve("b").build());
        assertEquals("test/a", PathBuilder.root("test")
                .resolve("b")
                .resolve("..")
                .resolve("a")
                .resolve(".")
                .resolve("")
                .build()
        );
        assertEquals("test/b/c", PathBuilder.root("test").resolve("b/a").resolve("../c/").build());
        assertEquals("a/b/c", PathBuilder.root("/a/b/c/").build());
        assertEquals("a/b/d", PathBuilder.root("./a/b").resolve("c/../d").build());
        assertEquals("c/a/b", PathBuilder.root("c").resolve("b").resolve("/a/b").build());
        assertEquals("test/a/", PathBuilder.root("test").resolve("a").buildDirPath());

        assertThrows(IllegalArgumentException.class, () ->
                PathBuilder.root("test").resolve("a").resolve("../..").build()
        );
    }

}