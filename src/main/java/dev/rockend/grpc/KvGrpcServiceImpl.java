package dev.rockend.grpc;


import com.google.protobuf.ByteString;

import dev.rockend.kv.proto.*;
import dev.rockend.repository.KvRepository;
import dev.rockend.util.KvEntry;
import dev.rockend.util.KvLookupResult;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class KvGrpcServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private static final int RANGE_BATCH_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(KvGrpcServiceImpl.class);
    private final KvRepository kvRepository;

    public KvGrpcServiceImpl(KvRepository kvRepository) {
        this.kvRepository = kvRepository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            String key = request.getKey();

            if (key == null || key.isBlank()) {
                log.info("key is null or blank");
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription("key is blank")
                                .asRuntimeException()
                );
                return;
            }

            byte[] value = null;

            if (request.hasValue()) {
                value = request.getValue().toByteArray();
            }

            kvRepository.save(key, value);
            log.info("save kv to repository");

            PutResponse response = PutResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Put kv to repository error", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            String key = request.getKey();

            if (key == null || key.isBlank()) {
                log.error("Key is null or blank");
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription("Key cannot be blank")
                                .asRuntimeException()
                );
                return;
            }


            GetResponse.Builder builder = GetResponse.newBuilder();


            KvLookupResult kvLookupResult = kvRepository.findByKey(key);
            byte[] value = kvLookupResult.value();

            if (!kvLookupResult.found()) {
                log.info("Key not found");
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("Key not found. Unable to retrieve the key")
                                .asRuntimeException()
                );
                return;
            }


            if (value != null) {
                builder.setValue(ByteString.copyFrom(value));
                log.info("Get kv from repository");
            }

            GetResponse response = builder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Get kv from repository error", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {

        try {

            String key = request.getKey();

            if (key == null || key.isBlank()) {
                log.error("Key is null or blank");
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription("Key cannot be blank. Deletion key failed")
                                .asRuntimeException()
                );
                return;
            }

            boolean deleted = kvRepository.delete(key);
            if (!deleted) {
                log.info("Key not found. Deletion key failed");
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("Key not found. Deletion key failed")
                                .asRuntimeException()
                );
                return;
            }

            DeleteResponse response = DeleteResponse.newBuilder().build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Get kv from repository error", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {

            long count = kvRepository.count();

            CountResponse response = CountResponse
                    .newBuilder()
                    .setCount(count)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Get records counter from repository error", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        var serverObserver = (io.grpc.stub.ServerCallStreamObserver<RangeResponse>) responseObserver;

        try {
            String keySince = request.getKeySince();
            String keyTo = request.getKeyTo();

            if (keySince == null || keySince.isBlank()) {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription("key_since cannot be blank")
                                .asRuntimeException()
                );
                return;
            }

            if (keyTo == null || keyTo.isBlank()) {
                responseObserver.onError(
                        Status.INVALID_ARGUMENT
                                .withDescription("key_to cannot be blank")
                                .asRuntimeException()
                );
                return;
            }

            String currentFrom = keySince;
            boolean includeSince = true;

            while (true) {
                if (serverObserver.isCancelled()) {
                    log.info("Range stream cancelled by client");
                    return;
                }

                var batch = kvRepository.rangeBatch(currentFrom, keyTo, RANGE_BATCH_SIZE, includeSince);

                if (batch.isEmpty()) {
                    break;
                }

                for (var entry : batch) {
                    if (serverObserver.isCancelled()) {
                        log.info("Range stream cancelled by client");
                        return;
                    }

                    RangeResponse.Builder builder = RangeResponse.newBuilder()
                            .setKey(entry.key());

                    if (entry.value() != null) {
                        builder.setValue(ByteString.copyFrom(entry.value()));
                    }

                    responseObserver.onNext(builder.build());
                }

                var lastEntry = batch.getLast();
                currentFrom = lastEntry.key();
                includeSince = false;

                if (batch.size() < RANGE_BATCH_SIZE) {
                    break;
                }
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            if (serverObserver.isCancelled()) {
                log.info("Range stream cancelled by client");
                return;
            }

            log.error("Range kv from repository error", e);
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription("Internal server error")
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
