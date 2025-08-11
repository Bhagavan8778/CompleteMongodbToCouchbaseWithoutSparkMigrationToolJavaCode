package com.demo.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class MongoDataFetchService {
    private final MongoConnectionService mongoConnectionService;

    @Autowired
    public MongoDataFetchService(MongoConnectionService mongoConnectionService) {
        this.mongoConnectionService = mongoConnectionService;
    }

    @SuppressWarnings("unchecked")
    @Retryable(value = {Exception.class}, maxAttempts = 3)
    public List<Map<String, Object>> fetchDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        MongoTemplate template = new MongoTemplate(mongoClient, dbName);
        return (List<Map<String, Object>>) (List<?>) template.findAll(Map.class, collectionName);
    }

    @Retryable(value = {Exception.class}, maxAttempts = 3)
    public Stream<Map<String, Object>> streamDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        return StreamSupport.stream(collection.find().spliterator(), false)
                .map(document -> (Map<String, Object>) document);
    }
    public long countDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        return mongoClient.getDatabase(dbName)
                          .getCollection(collectionName)
                          .countDocuments();
    }

}