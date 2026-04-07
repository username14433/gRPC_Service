package dev.rockend.client;

import com.google.protobuf.ByteString;
import dev.rockend.kv.proto.GetRequest;
import dev.rockend.kv.proto.GetResponse;
import dev.rockend.kv.proto.KvServiceGrpc;
import dev.rockend.kv.proto.PutRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;

public class KvManualClient_Old {

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        try {
            KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);

            // 1. put с null value
            stub.put(PutRequest.newBuilder()
                    .setKey("k1")
                    .build());

            GetResponse response1 = stub
                    .get(GetRequest.newBuilder()
                    .setKey("k1")
                    .build());

            System.out.println("k1 found, hasValue = " + response1.hasValue());

            // 2. put с пустым массивом байт
            stub.put(PutRequest.newBuilder()
                    .setKey("k2")
                    .setValue(ByteString.EMPTY)
                    .build());

            GetResponse response2 = stub.
                    get(GetRequest.newBuilder()
                    .setKey("k2")
                    .build());

            System.out.println("k2 hasValue = " + response2.hasValue());
            System.out.println("k2 value size = " + response2.getValue().size());
            System.out.println("k2 value = " + response2.getValue());

            // 3. отсутствующий ключ
            try {
                stub.get(GetRequest.newBuilder()
                        .setKey("missing")
                        .build());
            } catch (StatusRuntimeException e) {
                System.out.println("missing key status = " + e.getStatus().getCode());
            }

        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}