package com.s3.ftp.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PathBuilder {

    private final List<String> elements = new ArrayList<>();

    private final List<String> roots;

    public static PathBuilder root() {
        return root("");
    }

    public static PathBuilder root(String root) {
        return new PathBuilder(root);
    }

    private PathBuilder(String root) {
        this.roots = normalize(root).toList();
    }

    public PathBuilder resolve(String path) {
        if (!path.isEmpty() && path.charAt(0) == '/') {
            elements.clear();
        }

        normalize(path).forEach(elements::add);

        return this;
    }

    public String build() {
        List<String> result = new ArrayList<>();

        for (String ele : elements) {
            if (ele.equals("..")) {
                if (result.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Invalid path: %s".formatted(String.join("/", elements))
                    );
                }

                result.remove(result.size() - 1);
                continue;
            }

            result.add(ele);
        }

        return Stream.concat(roots.stream(), result.stream())
                .collect(Collectors.joining("/"));
    }

    private static Stream<String> normalize(String path) {
        return Arrays.stream(path.split("/"))
                .map(String::trim)
                .filter(ele -> !ele.isEmpty())
                .filter(ele -> !".".equals(ele));
    }

    public String buildDirPath() {
        return build() + "/";
    }

}
