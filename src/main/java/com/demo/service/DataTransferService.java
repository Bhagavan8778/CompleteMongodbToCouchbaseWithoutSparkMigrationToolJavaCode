package com.demo.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.demo.controller.MigrationProgressController;
import com.demo.dto.MigrationProgress;
import com.demo.exception.DatabaseTransferException;
import com.demo.util.DataTransformationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class DataTransferService {
    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);
    private static final int BATCH_SIZE = 1000;
    private static final String PROGRESS_TOPIC = "migration-progress";

    private final MongoDataFetchService mongoDataFetchService;
    private final CouchbaseConnectionService couchbaseConnectionService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    @Autowired
    private MigrationProgressController progressController;


    @Autowired
    public DataTransferService(MongoDataFetchService mongoDataFetchService,
                             CouchbaseConnectionService couchbaseConnectionService,
                             KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.mongoDataFetchService = mongoDataFetchService;
        this.couchbaseConnectionService = couchbaseConnectionService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void transferCollection(TransferRequest request) {
        AtomicInteger counter = new AtomicInteger(0);
        long totalDocs = mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection());

        Collection targetCollection = couchbaseConnectionService.getTargetCollection(
            request.bucketName(),
            request.scopeName(),
            request.collectionName()
        );

        // Send initial progress
        progressController.sendProgressUpdate(
            request.mongoDatabase(),
            request.mongoCollection(),
            0,
            (int) totalDocs,
            "STARTED"
        );

        mongoDataFetchService.streamDocuments(request.mongoDatabase(), request.mongoCollection())
            .forEach(document -> {
                try {
                    String id = document.get("_id").toString();
                    Map<String, Object> copy = new HashMap<>(document);
                    copy.remove("_id");
                    DataTransformationUtil.convertMongoTypes(copy);
                    targetCollection.upsert(id, copy);

                    int count = counter.incrementAndGet();
                    if (count % 100 == 0 || count == totalDocs) {
                        progressController.sendProgressUpdate(
                            request.mongoDatabase(),
                            request.mongoCollection(),
                            count,
                            (int) totalDocs,
                            "IN_PROGRESS"
                        );
                    }
                } catch (Exception e) {
                    logger.error("Error transferring document: {}", document.get("_id"), e);
                }
            });

        progressController.sendProgressUpdate(
            request.mongoDatabase(),
            request.mongoCollection(),
            counter.get(),
            (int) totalDocs,
            "COMPLETED"
        );
    }


    private void sendKafkaProgressUpdate(TransferRequest request, int transferred, long total, String status) {
        try {
            MigrationProgress progress = new MigrationProgress(
                request.mongoDatabase(),
                request.mongoCollection(),
                transferred,
                (int) total,
                status
            );
            String progressJson = objectMapper.writeValueAsString(progress);
            kafkaTemplate.send(PROGRESS_TOPIC, progressJson);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize progress update", e);
        }
    }

    public void transferCollection(String dbName, String sourceCollection, String scopeName, String targetCollection) {
        TransferRequest request = new TransferRequest(
            dbName, 
            sourceCollection,
            couchbaseConnectionService.getBucket().name(),
            scopeName, 
            targetCollection
        );
        transferCollection(request);
    }

    public void transferMultipleCollections(String dbName, Map<String, String> collectionsMap, String scopeName) {
        for (Map.Entry<String, String> entry : collectionsMap.entrySet()) {
            transferCollection(dbName, entry.getKey(), scopeName, entry.getValue());
        }
    }

    public void transferCollection1(String dbName, String sourceCollection, String scopeName, String targetCollection) {
        try {
            Bucket bucket = couchbaseConnectionService.getBucket();
            Scope scope = StringUtils.hasText(scopeName) ? bucket.scope(scopeName) : bucket.defaultScope();
            Collection collection = scope.collection(targetCollection);

            AtomicInteger counter = new AtomicInteger(0);

            mongoDataFetchService.streamDocuments(dbName, sourceCollection).forEach(document -> {
                try {
                    String id = document.get("_id").toString();
                    Map<String, Object> copy = new HashMap<>(document);
                    copy.remove("_id");
                    DataTransformationUtil.convertMongoTypes(copy);
                    collection.upsert(id, copy);

                    int count = counter.incrementAndGet();
                    if (count % BATCH_SIZE == 0) {
                        logger.info("Transferred {} documents from {}.{} to Couchbase", count, dbName, sourceCollection);
                        sendKafkaProgressUpdate(
                            new TransferRequest(dbName, sourceCollection, bucket.name(), scopeName, targetCollection),
                            count,
                            -1, // total unknown in this method
                            "BATCH_COMPLETED"
                        );
                    }
                } catch (Exception e) {
                    logger.error("Error transferring document: {}", document.get("_id"), e);
                }
            });

            logger.info("Completed transfer of {} documents from {}.{} to Couchbase", counter.get(), dbName, sourceCollection);
            sendKafkaProgressUpdate(
                new TransferRequest(dbName, sourceCollection, bucket.name(), scopeName, targetCollection),
                counter.get(),
                -1,
                "COMPLETED"
            );
        } catch (Exception e) {
            throw new DatabaseTransferException("Failed to transfer collection", e);
        }
    }

    public record TransferRequest(
        String mongoDatabase,
        String mongoCollection,
        String bucketName,
        String scopeName,
        String collectionName
    ) {}
}