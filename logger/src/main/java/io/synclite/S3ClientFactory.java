package io.synclite;

import java.net.URI;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;

public final class S3ClientFactory {
    private S3ClientFactory() {
    }

    public static S3Client create(String endpoint, String accessKey, String secretKey) {
        URI endpointUri = URI.create(endpoint);
        Region defaultRegion = Region.of(System.getProperty("aws.region",
                System.getenv("AWS_REGION") == null ? "us-east-1" : System.getenv("AWS_REGION")));
        Region region;
        try {
            region = S3Utilities.builder().region(defaultRegion).build().parseUri(endpointUri)
                    .region().orElse(defaultRegion);
        } catch (IllegalArgumentException exception) {
            region = defaultRegion;
        }
        String host = endpointUri.getHost();
        boolean awsEndpoint = host != null && (host.endsWith(".amazonaws.com") || host.endsWith(".amazonaws.com.cn"));
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .endpointOverride(endpointUri)
                .region(region)
                .forcePathStyle(!awsEndpoint)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
    }
}