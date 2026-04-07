package dev.rockend.client;

import com.google.protobuf.ByteString;
import dev.rockend.kv.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class KvManualClient {

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        try {
            KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);

            checkPutGetWithNullValue(stub);
            checkPutGetWithBytesValue(stub);
            checkDelete(stub);
            checkMissingKey(stub);
            checkBlankKeyValidation(stub);
            checkCount(stub);
            checkRange(stub);

            System.out.println("\nAll checks completed successfully.");
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void checkPutGetWithNullValue(KvServiceGrpc.KvServiceBlockingStub stub) {
        System.out.println("\n=== PUT/GET with null value ===");

        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("k1")
                .build();
        System.out.println("Send PUT: " + putRequest);
        stub.put(putRequest);
        System.out.println("PUT completed");

        GetRequest getRequest = GetRequest.newBuilder()
                .setKey("k1")
                .build();
        System.out.println("Send GET: " + getRequest);
        GetResponse response = stub.get(getRequest);
        System.out.println("GET response: hasValue = " + response.hasValue());

        check(!response.hasValue(), "Expected null value for key k1");
    }

    private static void checkPutGetWithBytesValue(KvServiceGrpc.KvServiceBlockingStub stub) {
        System.out.println("\n=== PUT/GET with byte[] value ===");

        byte[] expected = "hello".getBytes(StandardCharsets.UTF_8);

        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("k2")
                .setValue(ByteString.copyFrom(expected))
                .build();
        System.out.println("Send PUT: " + putRequest);
        stub.put(putRequest);
        System.out.println("PUT completed");

        GetRequest getRequest = GetRequest.newBuilder()
                .setKey("k2")
                .build();
        System.out.println("Send GET: " + getRequest);
        GetResponse response = stub.get(getRequest);
        System.out.println("GET response: hasValue = " + response.hasValue()
                + ", value = " + response.getValue().toString(StandardCharsets.UTF_8));

        check(response.hasValue(), "Expected non-null value for key k2");
        check(Arrays.equals(expected, response.getValue().toByteArray()), "Unexpected value for key k2");
    }

    private static void checkDelete(KvServiceGrpc.KvServiceBlockingStub stub) {
        System.out.println("\n=== DELETE ===");

        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setKey("k2")
                .build();
        System.out.println("Send DELETE: " + deleteRequest);
        stub.delete(deleteRequest);
        System.out.println("DELETE completed");

        try {
            System.out.println("Send GET after DELETE: key = k2");
            stub.get(GetRequest.newBuilder().setKey("k2").build());
            throw new IllegalStateException("Expected NOT_FOUND for deleted key k2");
        } catch (StatusRuntimeException e) {
            System.out.println("GET after DELETE error: " + e.getStatus().getCode());
            check(e.getStatus().getCode() == Status.Code.NOT_FOUND,
                    "Expected NOT_FOUND after delete");
        }
    }

    private static void checkMissingKey(KvServiceGrpc.KvServiceBlockingStub stub) {
        System.out.println("\n=== GET missing key ===");

        try {
            GetRequest request = GetRequest.newBuilder()
                    .setKey("missing")
                    .build();
            System.out.println("Send GET: " + request);
            stub.get(request);
            throw new IllegalStateException("Expected NOT_FOUND for missing key");
        } catch (StatusRuntimeException e) {
            System.out.println("GET missing key error: " + e.getStatus().getCode());
            check(e.getStatus().getCode() == Status.Code.NOT_FOUND,
                    "Expected NOT_FOUND for missing key");
        }
    }

    private static void checkBlankKeyValidation(KvServiceGrpc.KvServiceBlockingStub stub) {
        System.out.println("\n=== blank key validation ===");

        try {
            PutRequest request = PutRequest.newBuilder()
                    .setKey(" ")
                    .setValue(ByteString.copyFromUtf8("test"))
                    .build();
            System.out.println("Send PUT: " + request);
            stub.put(request);
            throw new IllegalStateException("Expected INVALID_ARGUMENT for blank key");
        } catch (StatusRuntimeException e) {
            System.out.println("PUT blank key error: " + e.getStatus().getCode());
            check(e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT,
                    "Expected INVALID_ARGUMENT for blank key");
        }
    }

    private static void checkCount(KvServiceGrpc.KvServiceBlockingStub stub) {

        CountResponse countBefore = stub.count(CountRequest.newBuilder().build());
        System.out.println("count before = " + countBefore.getCount());

        PutRequest putRequest = PutRequest.newBuilder()
                .setKey("count-test-key")
                .build();

        stub.put(putRequest);

        CountRequest emptyCountRequestAfterPut = CountRequest.newBuilder().build();

        CountResponse countAfterPut = stub.count(emptyCountRequestAfterPut);
        System.out.println("count after put = " + countAfterPut.getCount());

        DeleteRequest deleteRequest = DeleteRequest.newBuilder()
                .setKey("count-test-key")
                .build();

        stub.delete(deleteRequest);

        CountRequest emptyCountRequestAfterDelete = CountRequest.newBuilder().build();

        CountResponse countAfterDelete = stub.count(emptyCountRequestAfterDelete);
        System.out.println("count after delete = " + countAfterDelete.getCount());
    }

    public static void checkRange(KvServiceGrpc.KvServiceBlockingStub stub) {
        stub.put(PutRequest.newBuilder().setKey("a").build());
        stub.put(PutRequest.newBuilder().setKey("b").setValue(ByteString.copyFromUtf8("hello")).build());
        stub.put(PutRequest.newBuilder().setKey("c").setValue(ByteString.EMPTY).build());
        stub.put(PutRequest.newBuilder().setKey("z").build());

        Iterator<RangeResponse> iterator = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo("z")
                        .build()
        );

        while (iterator.hasNext()) {
            RangeResponse response = iterator.next();

            System.out.print("key = " + response.getKey());

            if (response.hasValue()) {
                System.out.println(", hasValue = true, valueSize = " + response.getValue().size());
            } else {
                System.out.println(", hasValue = false");
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
