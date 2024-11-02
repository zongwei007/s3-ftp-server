package com.s3.ftp.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class S3ClientRegistry {

    private final Map<String, Supplier<S3ClientBuilder>> clientProviders;

    public S3ClientRegistry(Properties properties) {
        this.clientProviders = resolveConfig(properties);
    }

    public S3ClientBuilder byKey(String storeKey) {
        return Optional.ofNullable(clientProviders.get(storeKey))
                .map(Supplier::get)
                .orElseThrow();
    }

    private Map<String, Supplier<S3ClientBuilder>> resolveConfig(Properties properties) {
        Map<String, Map<String, String>> configs = properties.entrySet().stream()
                .map(ele -> new String[]{ele.getKey().toString(), ele.getValue().toString()})
                .collect(Collectors.groupingBy(
                        v -> v[0].split("\\.")[1],
                        Collectors.reducing(
                                new HashMap<>(),
                                pair -> {
                                    String key = pair[0].replace("s3.", "");
                                    key = key.substring(key.indexOf('.') + 1);
                                    return Map.of(key, pair[1]);
                                },
                                (a, b) -> {
                                    HashMap<String, String> result = new HashMap<>();
                                    result.putAll(a);
                                    result.putAll(b);
                                    return Map.copyOf(result);
                                })
                ));

        return configs.keySet().stream()
                .collect(Collectors.toMap(Function.identity(), k -> new S3ClientSupplier(configs.get(k))));
    }

    private record S3ClientSupplier(Map<String, String> props) implements Supplier<S3ClientBuilder> {

        @Override
        public S3ClientBuilder get() {
            String endpoint = props.get("uri");
            String accessKey = props.get("access_key");
            String secretKey = props.get("secret_key");

            Region region = Optional.ofNullable(props.get("region"))
                    .map(Region::of).orElse(Region.US_EAST_1);

            return S3Client.builder()
                    .endpointOverride(URI.create(endpoint))
                    .forcePathStyle(true)
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            accessKey, secretKey
                    )));
        }
    }
}
