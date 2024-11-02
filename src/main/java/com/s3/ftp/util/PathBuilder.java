package com.s3.ftp.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class PathBuilder {

    private final List<String> elements = new ArrayList<>();

    public static PathBuilder from(String base) {
        return new PathBuilder(base);
    }

    private PathBuilder(String base) {
        resolve(base);
    }

    public PathBuilder resolve(String path) {
        Arrays.stream(path.split("/"))
                .map(String::trim)
                .filter(ele -> !ele.isEmpty())
                .forEach(elements::add);

        return this;
    }

    public String build() {
        List<String> result = new ArrayList<>();

        for (String ele : elements) {
            if (ele.equals("..")) {
                if (result.isEmpty()) {
                    throw new IllegalArgumentException("Invalid path: %s".formatted(String.join("/", elements)));
                }

                result.remove(result.size() - 1);
                continue;
            }

            result.add(ele);
        }

        return String.join("/", result);
    }
}
