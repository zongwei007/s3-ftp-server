package com.s3.ftp.config;

public record UserStoreInfo(String store, String bucket, String path) {

    public static UserStoreInfo fromHomeDirectory(String home) {
        String[] storeInfo = home.split(":");

        if (storeInfo.length != 2) {
            throw new IllegalArgumentException("Invalid home directory config: %s".formatted(home));
        }

        String store = storeInfo[0].trim();
        String bucket = storeInfo[1].trim();
        String path = "";

        if (bucket.contains("/")) {
            int pos = bucket.indexOf("/");
            path = bucket.substring(pos + 1).trim();
            bucket = bucket.substring(0, pos);
        }

        return new UserStoreInfo(store, bucket, path);
    }
}
