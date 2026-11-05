package io.synclite;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.hc.core5.http.impl.io.ChunkedInputStream;
import org.apache.hc.core5.http.impl.io.SessionInputBufferImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3ClientFactoryTest {
    private HttpServer server;
    private String previousRegion;
    private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
    private final List<String> authorizations = java.util.Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void startServer() throws IOException {
        previousRegion = System.getProperty("aws.region");
        System.setProperty("aws.region", "us-east-1");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
        if (previousRegion == null) System.clearProperty("aws.region");
        else System.setProperty("aws.region", previousRegion);
    }

    @Test
    void infersRegionalAwsEndpoints() {
        try (S3Client client = S3ClientFactory.create("https://s3.eu-west-2.amazonaws.com", "test-access", "test-secret")) {
            assertEquals(Region.EU_WEST_2, client.serviceClientConfiguration().region());
        }
        try (S3Client client = S3ClientFactory.create("https://s3.cn-north-1.amazonaws.com.cn", "test-access", "test-secret")) {
            assertEquals(Region.CN_NORTH_1, client.serviceClientConfiguration().region());
        }
    }

    @Test
    void customEndpointUploadsPaginatesDownloadsAndReportsMissingObjects() throws Exception {
        byte[] payload = "first".getBytes(StandardCharsets.UTF_8);
        try (S3Client client = S3ClientFactory.create(endpoint(), "test-access", "test-secret")) {
            client.putObject(request -> request.bucket("archive").key("device/first"), RequestBody.fromBytes(payload));
            client.putObject(request -> request.bucket("archive").key("device/second"), RequestBody.fromString("second"));
            client.putObject(request -> request.bucket("archive").key("device/"), RequestBody.empty());
            assertArrayEquals(payload, objects.get("/archive/device/first"));
            assertEquals(0, objects.get("/archive/device/").length);

            List<String> keys = new ArrayList<>();
            client.listObjectsV2Paginator(request -> request.bucket("archive").prefix("device/")).contents()
                    .forEach(object -> keys.add(object.key()));
            assertEquals(List.of("device/first", "device/second"), keys);

            try (var response = client.getObject(request -> request.bucket("archive").key("device/first"))) {
                assertArrayEquals(payload, response.readAllBytes());
                assertEquals(Instant.parse("2026-11-03T00:00:00Z"), response.response().lastModified());
            }
            S3Exception missing = assertThrows(S3Exception.class,
                    () -> client.getObject(request -> request.bucket("archive").key("missing")));
            assertEquals(404, missing.statusCode());
            assertEquals("NoSuchKey", missing.awsErrorDetails().errorCode());
            assertTrue(authorizations.stream().allMatch(value -> value.contains("/us-east-1/s3/aws4_request")));
        }
    }

    @Test
    void minioNineUsesCompatibleJvmHttpClientAndBoxedStreamSizes() throws Exception {
        byte[] payload = "minio".getBytes(StandardCharsets.UTF_8);
        try (MinioClient client = MinioClient.builder().endpoint(endpoint()).region("us-east-1")
                .credentials("test-access", "test-secret").build()) {
            client.putObject(PutObjectArgs.builder().bucket("archive").object("device/minio")
                    .stream(new ByteArrayInputStream(payload), (long) payload.length, -1L).build());
            client.putObject(PutObjectArgs.builder().bucket("archive").object("device/")
                    .stream(new ByteArrayInputStream(new byte[0]), 0L, -1L).build());
            assertArrayEquals(payload, objects.get("/archive/device/minio"));
            assertEquals(0, objects.get("/archive/device/").length);
            try (var response = client.getObject(GetObjectArgs.builder().bucket("archive").object("device/minio").build())) {
                assertArrayEquals(payload, response.readAllBytes());
            }
        }
    }

    private String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();
            exchange.getResponseHeaders().set("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"");
            exchange.getResponseHeaders().set("Last-Modified", "Tue, 03 Nov 2026 00:00:00 GMT");
            if ("PUT".equals(exchange.getRequestMethod())) {
                InputStream body = exchange.getRequestBody();
                String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
                if (encoding != null && encoding.contains("aws-chunked")) {
                    body = new ChunkedInputStream(new SessionInputBufferImpl(8192), body);
                }
                objects.put(path, body.readAllBytes());
                exchange.sendResponseHeaders(200, -1);
            } else if (query != null && query.contains("list-type=2")) {
                boolean secondPage = query.contains("continuation-token=page-2");
                String key = secondPage ? "device/second" : "device/first";
                String xml = "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        + "<Name>archive</Name><Prefix>device/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>"
                        + "<IsTruncated>" + !secondPage + "</IsTruncated>"
                        + (secondPage ? "" : "<NextContinuationToken>page-2</NextContinuationToken>")
                        + "<Contents><Key>" + key + "</Key><Size>5</Size>"
                        + "<LastModified>2026-11-03T00:00:00Z</LastModified></Contents></ListBucketResult>";
                respond(exchange, 200, xml.getBytes(StandardCharsets.UTF_8), "application/xml");
            } else if (objects.containsKey(path)) {
                respond(exchange, 200, objects.get(path), "application/octet-stream");
            } else {
                respond(exchange, 404, "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>"
                        .getBytes(StandardCharsets.UTF_8), "application/xml");
            }
        } finally {
            exchange.close();
        }
    }

    private void respond(HttpExchange exchange, int status, byte[] body, String contentType) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }
}