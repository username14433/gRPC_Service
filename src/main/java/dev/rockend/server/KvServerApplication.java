package dev.rockend.server;

import dev.rockend.grpc.KvGrpcServiceImpl;
import dev.rockend.repository.KvRepository;
import dev.rockend.repository.KvRepositoryImpl;
import dev.rockend.tarantool.TarantoolClientProvider;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.tarantool.client.box.TarantoolBoxClient;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KvServerApplication {

    private final int port;
    private final Server server;

    public KvServerApplication(int port, KvRepository kvRepository) {
        this.port = port;
        this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new KvGrpcServiceImpl(kvRepository))
                .build();
    }

    public void start() throws IOException {
        server.start();
        log.info("gRPC server started, listening on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("JVM shutdown detected, stopping gRPC server...");
            try {
                stop();
            } catch (InterruptedException e) {
                log.error("Error while stopping gRPC server", e);
                Thread.currentThread().interrupt();
            }
            log.info("gRPC server stopped");
        }));
    }

    public void stop() throws InterruptedException {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }

    public void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        int serverPort = 9090;


        TarantoolBoxClient client =
                TarantoolClientProvider.create("localhost", 3301, "app", "app");

        KvRepository repository = new KvRepositoryImpl(client);
        KvServerApplication app = new KvServerApplication(serverPort, repository);

        app.start();
        app.blockUntilShutdown();
    }
}