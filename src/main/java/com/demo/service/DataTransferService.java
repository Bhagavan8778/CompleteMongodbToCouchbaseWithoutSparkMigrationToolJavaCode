package com.demo.service;
 
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.demo.controller.MigrationProgressController;
import com.demo.dto.MigrationProgress;
import com.demo.util.DataTransformationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
 
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
 
@Service
public class DataTransferService {
 
    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);
 
    private static final int BATCH_SIZE = 4000;
    private static final int CONCURRENCY_LEVEL = 3000;
 
    private static final String PROGRESS_TOPIC = "migration-progress";
 
    private final MongoDataFetchService mongoDataFetchService;
    private final CouchbaseConnectionService couchbaseConnectionService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MigrationProgressController progressController;
    private final CheckpointService checkpointService;
 
    private volatile boolean paused = false;
    private volatile boolean connectionLost = false;
 
    @Autowired
    public DataTransferService(MongoDataFetchService mongoDataFetchService,
                               CouchbaseConnectionService couchbaseConnectionService,
                               KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               MigrationProgressController progressController,
                               CheckpointService checkpointService) {
        this.mongoDataFetchService = mongoDataFetchService;
        this.couchbaseConnectionService = couchbaseConnectionService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.progressController = progressController;
        this.checkpointService = checkpointService;
    }
 
 
    //--------- Connection helpers -----------
    private boolean isMongoConnected() {
        try {
            mongoDataFetchService.ping();
            return true;
        } catch (Exception e) {
            logger.warn("Mongo ping failed: {}", e.getMessage());
            return false;
        }
    }
 
    private boolean isCouchbaseConnected() {
        try {
            couchbaseConnectionService.ping();
            return true;
        } catch (Exception e) {
            logger.warn("Couchbase ping failed: {}", e.getMessage());
            return false;
        }
    }
 
    // Waits until both are available.
    private void waitUntilConnectionsRestored() {
        logger.warn("Transfer paused. Waiting for both MongoDB & Couchbase connections...");
        int logCounter = 0;
        while (!isMongoConnected() || !isCouchbaseConnected()) {
            paused = true;
            connectionLost = true;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {}
            if (++logCounter % 2 == 0) {
                logger.warn("Still waiting for DB connections to come back...");
            }
        }
        logger.info("Connections restored. Resuming transfer...");
        paused = false;
        connectionLost = false;
    }
 
    private void handleConnectionLost(TransferRequest request, int count, int total, String message) {
        logger.warn("Connection lost, pausing at doc {} of {}. Message: {}", count, total, message);
        sendProgressUpdate(request, count, total, "CONNECTION_LOST");
    }
 
    //--------- Retry helpers -----------
    private <T> T executeMongoOperationWithRetry(Supplier<T> operation, int maxRetries) {
        int attempts = 0;
        while (true) {
            try {
                return operation.get();
            } catch (MongoTimeoutException ex) {
                attempts++;
                connectionLost = true;
                paused = true;
                logger.warn("MongoTimeoutException - connection lost: {}", ex.getMessage());
                throw ex;
            } catch (Exception ex) {
                if (isRetryable(ex)) {
                    attempts++;
                    if (attempts > maxRetries) {
                        logger.error("Mongo operation failed after {} retries.", maxRetries);
                        connectionLost = true;
                        paused = true;
                        throw ex;
                    }
                    int sleepMillis = 1000 * attempts * attempts;
                    logger.warn("Retryable Mongo exception: {}, attempt {} after {} ms...", ex.toString(), attempts, sleepMillis);
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry backoff", ie);
                    }
                } else {
                    throw new RuntimeException("Mongo operation failed with non-retryable error", ex);
                }
            }
        }
    }
 
    private Mono<MutationResult> upsertWithRetry(ReactiveCollection reactiveCollection,
                                                 String id,
                                                 JsonObject jsonDoc,
                                                 int maxRetries) {
        return Mono.defer(() -> reactiveCollection.upsert(id, jsonDoc))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(500))
                        .filter(this::isRetryable))
                .onErrorResume(e -> {
                    logger.error("Final failure upserting doc {} after retries: {}", id, e.toString());
                    paused = true; connectionLost = true;
                    return Mono.empty();
                });
    }
 
    private boolean isRetryable(Throwable e) {
        return
            e instanceof com.couchbase.client.core.error.TimeoutException ||
            e instanceof com.couchbase.client.core.error.AmbiguousTimeoutException ||
            e instanceof com.couchbase.client.core.error.RequestCanceledException ||
            e instanceof MongoTimeoutException ||
            (e.getCause() != null && isRetryable(e.getCause()));
    }
 
    //--------- Progress reporting -----------
    private void sendProgressUpdate(TransferRequest request, int transferred, int total, String status) {
        progressController.sendProgressUpdate(
                request.mongoDatabase(),
                request.mongoCollection(),
                transferred,
                total,
                status
        );
 
        try {
            MigrationProgress progress = new MigrationProgress(
                    request.mongoDatabase(),
                    request.mongoCollection(),
                    transferred,
                    total,
                    status
            );
            String progressJson = objectMapper.writeValueAsString(progress);
            kafkaTemplate.send(PROGRESS_TOPIC, progressJson);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize progress update", e);
        }
    }
 
    public record TransferRequest(
            String mongoDatabase,
            String mongoCollection,
            String bucketName,
            String scopeName,
            String collectionName
    ) {}
 
    // ---- Checkpointed transfer with pause/resume ----
    public void transferCollectionWithDocumentCheckpoints(TransferRequest request, String checkpointId) {
        paused = false;
        connectionLost = false;
        long startTime = System.currentTimeMillis();
 
        Checkpoint lastCheckpoint = checkpointService.loadCheckpoint(checkpointId);
        String lastProcessedId = lastCheckpoint != null ? lastCheckpoint.getLastProcessedId() : null;
        int alreadySucceeded = lastCheckpoint != null ? lastCheckpoint.getTotalSucceeded() : 0;
 
        long totalDocs;
        try {
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        } catch (Exception e) {
            handleConnectionLost(request, alreadySucceeded, 0, "MongoDB connection lost at start.");
            waitUntilConnectionsRestored();
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        }
 
        Collection targetCollection;
        try {
            targetCollection = couchbaseConnectionService.getTargetCollection(
                    request.bucketName(), request.scopeName(), request.collectionName());
        } catch (Exception e) {
            handleConnectionLost(request, alreadySucceeded, 0, "Couchbase connection lost at start.");
            waitUntilConnectionsRestored();
            targetCollection = couchbaseConnectionService.getTargetCollection(
                    request.bucketName(), request.scopeName(), request.collectionName());
        }
 
        sendProgressUpdate(request, alreadySucceeded, (int) totalDocs, "STARTED");
 
        long numBatches = (totalDocs / BATCH_SIZE) + 1;
        AtomicInteger successCounter = new AtomicInteger(alreadySucceeded);
 
        boolean resumeMode = lastProcessedId != null;
        boolean foundLastId = !resumeMode;
 
        outerLoop:
        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            List<Map<String, Object>> batch = null;
            while(batch == null) {
                try {
                    batch = mongoDataFetchService.fetchBatch(
                                request.mongoDatabase(),
                                request.mongoCollection(),
                                batchIndex * BATCH_SIZE,
                                BATCH_SIZE);
                    if (connectionLost) {
                        sendProgressUpdate(request, successCounter.get(), (int)totalDocs, "RESUMED");
                        connectionLost = false;
                        paused = false;
                    }
                } catch (Exception ex) {
                    handleConnectionLost(request, successCounter.get(), (int)totalDocs, "MongoDB connection lost during batch fetch.");
                    waitUntilConnectionsRestored();
                }
            }
 
            if (resumeMode && !foundLastId) {
                int idx = 0;
                for (; idx < batch.size(); idx++) {
                    Map<String, Object> doc = batch.get(idx);
                    if (doc.get("_id").toString().equals(lastProcessedId)) {
                        idx++;
                        foundLastId = true;
                        break;
                    }
                }
                if (!foundLastId) continue;
                batch = batch.subList(idx, batch.size());
            }
 
            String newLastId = null;
            ReactiveCollection reactiveCollection = targetCollection.reactive();
 
            for (Map<String, Object> document : batch) {
                boolean upsertSuccess = false;
                int retryCount = 0;
 
                while (!upsertSuccess && retryCount <= 5) {
                    if (paused || connectionLost) {
                        handleConnectionLost(request, successCounter.get(), (int)totalDocs, "Connection lost during upsert operation (document-level).");
                        waitUntilConnectionsRestored();
                    }
                    try {
                        String id = document.get("_id").toString();
                        Map<String, Object> copy = new HashMap<>(document);
                        copy.remove("_id");
                        DataTransformationUtil.convertMongoTypes(copy);
                        JsonObject jsonDoc = JsonObject.from(copy);
 
                        upsertWithRetry(reactiveCollection, id, jsonDoc, 3).block();
 
                        newLastId = id;
                        int done = successCounter.incrementAndGet();
 
                        if (done % 1000 == 0 || done == totalDocs) {
                            sendProgressUpdate(request, done, (int) totalDocs, "IN_PROGRESS");
                        }
 
                        upsertSuccess = true;
                    } catch (Exception e) {
                        retryCount++;
                        logger.error("Error upserting doc id {} attempt {}: {}", document.get("_id"), retryCount, e.toString());
                        if (e instanceof MongoTimeoutException || isRetryable(e)) {
                            paused = true; connectionLost = true;
                            handleConnectionLost(request, successCounter.get(), (int) totalDocs, "Connection lost during document upsert.");
                            waitUntilConnectionsRestored();
                        } else {
                            logger.error("Non-retryable error for doc id {}, skipping", document.get("_id"));
                            upsertSuccess = true; // skip doc
                        }
                    }
                }
            }
 
            // Save checkpoint at end of every batch
            if (newLastId != null) {
                Checkpoint checkpoint = new Checkpoint(
                        checkpointId,
                        "DOCUMENT_TRANSFER",
                        successCounter.get(),
                        0,
                        successCounter.get(),
                        0,
                        new HashSet<>(),
                        new HashSet<>(),
                        newLastId);
                checkpointService.saveCheckpoint(checkpoint);
            }
        }
 
        sendProgressUpdate(request, successCounter.get(), (int) totalDocs, "COMPLETED");
        checkpointService.deleteCheckpoint(checkpointId);
 
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Checkpointed transfer completed: {} documents transferred in {} ms ({} docs/sec).",
                successCounter.get(), duration, (successCounter.get() * 1000) / Math.max(duration, 1));
    }
 
 
    // ---- Basic transfer (no checkpoint), also with pause/resume ----
    public void transferCollection(TransferRequest request) {
        long startTime = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger(0);
 
        long totalDocs;
        try {
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        } catch (Exception e) {
            handleConnectionLost(request, counter.get(), 0, "MongoDB connection lost at start.");
            waitUntilConnectionsRestored();
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        }
 
        Collection targetCollection;
        try {
            targetCollection = couchbaseConnectionService.getTargetCollection(
                    request.bucketName(), request.scopeName(), request.collectionName());
        } catch (Exception e) {
            handleConnectionLost(request, counter.get(), 0, "Couchbase connection lost at start.");
            waitUntilConnectionsRestored();
            targetCollection = couchbaseConnectionService.getTargetCollection(
                    request.bucketName(), request.scopeName(), request.collectionName());
        }
 
        sendProgressUpdate(request, 0, (int) totalDocs, "STARTED");
 
        long numBatches = (totalDocs / BATCH_SIZE) + 1;
 
        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            List<Map<String, Object>> batch = null;
            while (batch == null) {
                try {
                    batch = mongoDataFetchService.fetchBatch(
                            request.mongoDatabase(),
                            request.mongoCollection(),
                            batchIndex * BATCH_SIZE,
                            BATCH_SIZE);
                    if (connectionLost) {
                        sendProgressUpdate(request, counter.get(), (int) totalDocs, "RESUMED");
                        connectionLost = false;
                        paused = false;
                    }
                } catch (Exception ex) {
                    handleConnectionLost(request, counter.get(), (int)totalDocs, "MongoDB connection lost during batch fetch.");
                    waitUntilConnectionsRestored();
                }
            }
            writeBatchReactiveWithPause(batch, targetCollection, counter, request, totalDocs);
        }
        sendProgressUpdate(request, counter.get(), (int) totalDocs, "COMPLETED");
 
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Transferred {} documents in {} ms ({} docs/sec)", counter.get(), duration, (counter.get() * 1000) / Math.max(duration, 1));
    }
 
    private void writeBatchReactiveWithPause(List<Map<String, Object>> docs,
                                             Collection targetCollection,
                                             AtomicInteger counter,
                                             TransferRequest request,
                                             long totalDocs) {
        ReactiveCollection reactiveCollection = targetCollection.reactive();
 
        Function<Map<String, Object>, Mono<MutationResult>> mapper = document -> {
            return Mono.defer(() -> {
                int retries = 0;
                while (true) {
                    if (paused || connectionLost) {
                        handleConnectionLost(request, counter.get(), (int) totalDocs, "Connection lost during batch reactive upsert.");
                        waitUntilConnectionsRestored();
                    }
                    try {
                        String id = document.get("_id").toString();
                        Map<String, Object> copy = new HashMap<>(document);
                        copy.remove("_id");
                        DataTransformationUtil.convertMongoTypes(copy);
                        JsonObject jsonDoc = JsonObject.from(copy);
 
                        return upsertWithRetry(reactiveCollection, id, jsonDoc, 3);
                    } catch (Exception e) {
                        retries++;
                        logger.error("Error upserting doc id {} attempt {}: {}", document.get("_id"), retries, e.toString());
                        if (e instanceof MongoTimeoutException || isRetryable(e)) {
                            paused = true; connectionLost = true;
                            handleConnectionLost(request, counter.get(), (int) totalDocs, "Connection lost during upsert.");
                            waitUntilConnectionsRestored();
                        } else {
                            logger.error("Non-retryable error for doc id {}, skipping", document.get("_id"));
                            return Mono.empty();
                        }
                    }
                }
            });
        };
 
        Flux.fromIterable(docs)
                .parallel(CONCURRENCY_LEVEL)
                .runOn(Schedulers.boundedElastic())
                .flatMap(mapper)
                .sequential()
                .doOnNext(result -> {
                    int count = counter.incrementAndGet();
                    if (count % 1000 == 0 || count == totalDocs) {
                        sendProgressUpdate(request, count, (int) totalDocs, "IN_PROGRESS");
                    }
                })
                .blockLast();
    }
}