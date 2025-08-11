//package com.demo.service;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoDatabase;
//import org.bson.Document;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
//@Service
//public class MongoMetadataService {
//
//    private final MongoConnectionService mongoConnectionService;
//    private static final double BYTES_TO_KB = 1024;
//    private static final double BYTES_TO_MB = 1024 * 1024;
//    private static final double BYTES_TO_GB = 1024 * 1024 * 1024;
//    private static final double BYTES_TO_TB = 1024 * 1024 * 1024 * 1024L;
//
//    @Autowired
//    public MongoMetadataService(MongoConnectionService mongoConnectionService) {
//        this.mongoConnectionService = mongoConnectionService;
//    }
//
//    public List<Map<String, Object>> getDatabaseMetadata() {
//        MongoClient mongoClient = mongoConnectionService.getMongoClient();
//        List<Map<String, Object>> dbMetadataList = new ArrayList<>();
//        int userFunctionCount = getUserDefinedFunctionsCount(mongoClient);
//        
//        for (String dbName : mongoClient.listDatabaseNames()) {
//            if (dbName.equals("local") || dbName.equals("admin") || dbName.equals("config")) {
//                continue;
//            }
//
//            Map<String, Object> dbInfo = new LinkedHashMap<>();
//            dbInfo.put("databaseName", dbName);  
//            
//            try {
//                MongoDatabase db = mongoClient.getDatabase(dbName);
//                Document dbStats = db.runCommand(new Document("dbStats", 1));
//                
//              
//                dbInfo.put("totalDocuments", getLongValue(dbStats, "objects"));
//                dbInfo.put("totalCollections", getLongValue(dbStats, "collections"));
//                dbInfo.put("totalDataSize", formatStorageSize(getDoubleValue(dbStats, "dataSize")));
//                dbInfo.put("storageSize", formatStorageSize(getDoubleValue(dbStats, "storageSize")));
//                dbInfo.put("totalIndexSize", formatStorageSize(getDoubleValue(dbStats, "indexSize")));
//                dbInfo.put("totalIndexes", getLongValue(dbStats, "indexes"));
//                dbInfo.put("userDefinedFunctions", userFunctionCount);
//                
//               
//                dbInfo.put("collections", getCollectionsMetadata(db));
//                dbInfo.put("host", mongoClient.getClusterDescription().getServerDescriptions()
//                    .stream()
//                    .findFirst()
//                    .map(s -> s.getAddress().toString())
//                    .orElse("unknown"));
//                
//            } catch (Exception e) {
//                dbInfo.put("error", "Failed to get stats: " + e.getMessage());
//                try {
//                    dbInfo.put("collections", mongoClient.getDatabase(dbName)
//                        .listCollectionNames()
//                        .into(new ArrayList<>())
//                        .stream()
//                        .map(name -> Map.of("collectionName", name))
//                        .collect(Collectors.toList()));
//                } catch (Exception ex) {
//                    dbInfo.put("collections", Collections.emptyList());
//                }
//            }
//            
//            dbMetadataList.add(dbInfo);
//        }
//        
//        return dbMetadataList;
//    }
//
//    private List<Map<String, Object>> getCollectionsMetadata(MongoDatabase db) {
//        List<Map<String, Object>> collections = new ArrayList<>();
//        
//        for (String collName : db.listCollectionNames()) {
//            Map<String, Object> collInfo = new LinkedHashMap<>();
//            collInfo.put("collectionName", collName);
//            
//            try {
//                Document collStats = db.runCommand(new Document("collStats", collName));
//                collInfo.put("documentCount", getLongValue(collStats, "count"));
//                collInfo.put("totalSize", formatStorageSize(getDoubleValue(collStats, "size")));
//                collInfo.put("storageSize", formatStorageSize(getDoubleValue(collStats, "storageSize")));
//                collInfo.put("averageDocumentSize", formatStorageSize(getDoubleValue(collStats, "avgObjSize")));
//                collInfo.put("indexCount", collStats.get("nindexes", 0));
//                collInfo.put("totalIndexSize", formatStorageSize(getDoubleValue(collStats, "totalIndexSize")));
//                
//            } catch (Exception e) {
//                collInfo.put("error", "Failed to get stats: " + e.getMessage());
//                try {
//                    collInfo.put("documentCount", 
//                        db.getCollection(collName).countDocuments());
//                } catch (Exception ex) {
//                    collInfo.put("documentCount", "unavailable");
//                }
//            }
//            
//            collections.add(collInfo);
//        }
//        
//        return collections;
//    }
//
//    private int getUserDefinedFunctionsCount(MongoClient mongoClient) {
//        try {
//            Document result = mongoClient.getDatabase("admin")
//                .runCommand(new Document("listCommands", 1));
//            Document commands = result.get("commands", Document.class);
//            return commands != null ? commands.size() : 0;
//        } catch (Exception e) {
//            return 0; 
//        }
//    }
//
//    private Map<String, Object> formatStorageSize(double bytes) {
//        Map<String, Object> sizeInfo = new LinkedHashMap<>();
//        
//        if (bytes >= BYTES_TO_TB) {
//            sizeInfo.put("value", roundToTwoDecimals(bytes / BYTES_TO_TB));
//            sizeInfo.put("unit", "TB");
//        } else if (bytes >= BYTES_TO_GB) {
//            sizeInfo.put("value", roundToTwoDecimals(bytes / BYTES_TO_GB));
//            sizeInfo.put("unit", "GB");
//        } else if (bytes >= BYTES_TO_MB) {
//            sizeInfo.put("value", roundToTwoDecimals(bytes / BYTES_TO_MB));
//            sizeInfo.put("unit", "MB");
//        } else if (bytes >= BYTES_TO_KB) {
//            sizeInfo.put("value", roundToTwoDecimals(bytes / BYTES_TO_KB));
//            sizeInfo.put("unit", "KB");
//        } else {
//            sizeInfo.put("value", roundToTwoDecimals(bytes));
//            sizeInfo.put("unit", "bytes");
//        }
//        
//        sizeInfo.put("rawBytes", bytes);
//        return sizeInfo;
//    }
//
//    private double getDoubleValue(Document doc, String key) {
//        Object value = doc.get(key);
//        if (value instanceof Number) {
//            return ((Number) value).doubleValue();
//        }
//        return 0.0;
//    }
//
//    private long getLongValue(Document doc, String key) {
//        Object value = doc.get(key);
//        if (value instanceof Number) {
//            return ((Number) value).longValue();
//        }
//        return 0L;
//    }
//
//    private double roundToTwoDecimals(double value) {
//        return Math.round(value * 100) / 100.0;
//    }
//}

//
//package com.demo.service;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoDatabase;
//import org.bson.Document;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class MongoMetadataService {
//
//    private final MongoConnectionService mongoConnectionService;
//
//    @Autowired
//    public MongoMetadataService(MongoConnectionService mongoConnectionService) {
//        this.mongoConnectionService = mongoConnectionService;
//    }
//
//    public Map<String, Object> getTotalStats() {
//        MongoClient mongoClient = mongoConnectionService.getMongoClient();
//        Map<String, Object> stats = new LinkedHashMap<>();
//        
//        long totalDocuments = 0;
//        long totalCollections = 0;
//        double totalDataSizeBytes = 0;
//        int totalDatabases = 0;
//
//        // Get database names as a list first to avoid multiple iterations
//        List<String> databaseNames = new ArrayList<>();
//        mongoClient.listDatabaseNames().into(databaseNames);
//
//        for (String dbName : databaseNames) {
//            if (dbName.equals("local") || dbName.equals("admin") || dbName.equals("config")) {
//                continue;
//            }
//            totalDatabases++;
//
//            try {
//                MongoDatabase db = mongoClient.getDatabase(dbName);
//                Document dbStats = db.runCommand(new Document("dbStats", 1));
//                
//                totalDocuments += dbStats.getLong("objects");
//                totalCollections += dbStats.getLong("collections");
//                totalDataSizeBytes += dbStats.getDouble("dataSize");
//                
//            } catch (Exception e) {
//                // Log the error if needed
//                continue;
//            }
//        }
//
//        stats.put("totalDatabases", totalDatabases);
//        stats.put("totalCollections", totalCollections);
//        stats.put("totalDocuments", totalDocuments);
//        stats.put("totalDataSize", formatSize(totalDataSizeBytes));
//        
//        return stats;
//    }
//
//    private Map<String, Object> formatSize(double bytes) {
//        Map<String, Object> sizeInfo = new LinkedHashMap<>();
//        sizeInfo.put("bytes", bytes);
//        
//        if (bytes >= 1024 * 1024 * 1024) {
//            sizeInfo.put("gb", round(bytes / (1024 * 1024 * 1024)));
//            sizeInfo.put("mb", round(bytes / (1024 * 1024)));
//        } else if (bytes >= 1024 * 1024) {
//            sizeInfo.put("mb", round(bytes / (1024 * 1024)));
//            sizeInfo.put("kb", round(bytes / 1024));
//        } else if (bytes >= 1024) {
//            sizeInfo.put("kb", round(bytes / 1024));
//        }
//        
//        return sizeInfo;
//    }
//
//    private double round(double value) {
//        return Math.round(value * 100) / 100.0;
//    }
//}


// this is the details of the total documents,collections and databases 

//package com.demo.service;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoDatabase;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class MongoMetadataService {
//    private static final Logger logger = LoggerFactory.getLogger(MongoMetadataService.class);
//    private final MongoConnectionService mongoConnectionService;
//
//    @Autowired
//    public MongoMetadataService(MongoConnectionService mongoConnectionService) {
//        this.mongoConnectionService = mongoConnectionService;
//    }
//
//    public Map<String, Object> getStorageMetrics() {
//        MongoClient mongoClient = mongoConnectionService.getMongoClient();
//        Map<String, Object> metrics = new LinkedHashMap<>();
//        
//        long totalDocuments = 0;
//        long totalCollections = 0;
//        long totalStorageBytes = 0;
//        long totalDataBytes = 0; // For logical data size
//        long totalIndexBytes = 0;
//        int totalDatabases = 0;
//
//        try {
//            List<Document> databaseInfos = mongoClient.listDatabases().into(new ArrayList<>());
//            
//            for (Document dbInfo : databaseInfos) {
//                String dbName = dbInfo.getString("name");
//                
//                if (shouldSkipDatabase(dbName)) {
//                    continue;
//                }
//                
//                totalDatabases++;
//                MongoDatabase db = mongoClient.getDatabase(dbName);
//                
//                try {
//                    Document dbStats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
//                    
//                    // Core metrics
//                    totalDocuments += getSafeLong(dbStats, "objects");
//                    totalCollections += getSafeLong(dbStats, "collections");
//                    
//                    // Storage metrics
//                    totalStorageBytes += getSafeLong(dbStats, "storageSize");
//                    totalDataBytes += getSafeLong(dbStats, "dataSize"); // Logical data size
//                    totalIndexBytes += getSafeLong(dbStats, "indexSize");
//                    
//                } catch (Exception e) {
//                    logger.warn("dbStats failed for {}, using fallback: {}", dbName, e.getMessage());
//                    CollectionStatsResult result = getCollectionLevelStats(db);
//                    totalDocuments += result.docCount;
//                    totalCollections += result.collectionCount;
//                    totalStorageBytes += result.storageSize;
//                    totalDataBytes += result.dataSize;
//                    totalIndexBytes += result.indexSize;
//                }
//            }
//        } catch (Exception e) {
//            logger.error("Failed to fetch database list: {}", e.getMessage());
//            throw new RuntimeException("Database connection error", e);
//        }
//
//        // Format results with both storage and logical sizes
//        metrics.put("totalDatabases", totalDatabases);
//        metrics.put("totalCollections", totalCollections);
//        metrics.put("totalDocuments", totalDocuments);
//        
//        metrics.put("logicalDataSize", formatSize(totalDataBytes)); // 200.37MB
//        metrics.put("storageSize", formatSize(totalStorageBytes));  // 56.17MB
//        metrics.put("indexSize", formatSize(totalIndexBytes));      // 136KB
//        metrics.put("compressionRatio", calculateCompressionRatio(totalDataBytes, totalStorageBytes));
//        
//        return metrics;
//    }
//
//    private CollectionStatsResult getCollectionLevelStats(MongoDatabase db) {
//        CollectionStatsResult result = new CollectionStatsResult();
//        List<String> collectionNames = db.listCollectionNames().into(new ArrayList<>());
//        result.collectionCount = collectionNames.size();
//
//        for (String collName : collectionNames) {
//            try {
//                Document collStats = db.runCommand(new Document("collStats", collName));
//                result.docCount += getSafeLong(collStats, "count");
//                result.storageSize += getSafeLong(collStats, "storageSize");
//                result.dataSize += getSafeLong(collStats, "size"); // Logical size
//                result.indexSize += getSafeLong(collStats, "totalIndexSize");
//            } catch (Exception e) {
//                logger.warn("collStats failed for {}, using countDocuments: {}", collName, e.getMessage());
//                try {
//                    result.docCount += db.getCollection(collName).countDocuments();
//                } catch (Exception ex) {
//                    logger.error("countDocuments failed for {}: {}", collName, ex.getMessage());
//                }
//            }
//        }
//        return result;
//    }
//
//    private String formatSize(long bytes) {
//        if (bytes < 1024) return bytes + " bytes";
//        if (bytes < 1024 * 1024) return (bytes / 1024) + " KB";
//        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
//        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
//    }
//
//    private double calculateCompressionRatio(long logicalSize, long storageSize) {
//        if (storageSize == 0) return 0;
//        return Math.round((logicalSize / (double) storageSize) * 100) / 100.0;
//    }
//
//    private boolean shouldSkipDatabase(String dbName) {
//        return dbName.equals("admin") || 
//               dbName.equals("local") || 
//               dbName.equals("config") ||
//               dbName.startsWith("system.");
//    }
//
//    private long getSafeLong(Document doc, String key) {
//        try {
//            Object value = doc.get(key);
//            return (value instanceof Number) ? ((Number) value).longValue() : 0L;
//        } catch (Exception e) {
//            return 0L;
//        }
//    }
//
//    private static class CollectionStatsResult {
//        long docCount = 0;
//        long storageSize = 0;
//        long dataSize = 0; // For logical data size
//        long indexSize = 0;
//        int collectionCount = 0;
//    }
//}


// this is without system collections and documents

package com.demo.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class MongoMetadataService {
    private static final Logger logger = LoggerFactory.getLogger(MongoMetadataService.class);
    private final MongoConnectionService mongoConnectionService;

    @Autowired
    public MongoMetadataService(MongoConnectionService mongoConnectionService) {
        this.mongoConnectionService = mongoConnectionService;
    }

    public Map<String, Object> getStorageMetrics() {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        Map<String, Object> metrics = new LinkedHashMap<>();
        
        long totalDocuments = 0;
        long totalCollections = 0;
        long totalStorageBytes = 0;
        long totalDataBytes = 0;
        long totalIndexBytes = 0;
        int totalDatabases = 0;

        try {
            // Get all databases excluding system databases
            List<Document> databaseInfos = mongoClient.listDatabases().into(new ArrayList<>())
                .stream()
                .filter(dbInfo -> !isSystemDatabase(dbInfo.getString("name")))
                .collect(Collectors.toList());
            
            for (Document dbInfo : databaseInfos) {
                String dbName = dbInfo.getString("name");
                totalDatabases++;
                MongoDatabase db = mongoClient.getDatabase(dbName);
                
                try {
                    // First try to get stats at database level (faster)
                    Document dbStats = db.runCommand(new Document("dbStats", 1).append("scale", 1));
                    
                    // Get user collections count (excluding system collections)
                    long userCollectionsCount = db.listCollectionNames()
                        .into(new ArrayList<>())
                        .stream()
                        .filter(this::isUserCollection)
                        .count();
                    
                    totalCollections += userCollectionsCount;
                    
                    if (userCollectionsCount > 0) {
                        // If we have user collections, get their stats
                        CollectionStatsResult result = getCollectionLevelStats(db);
                        totalDocuments += result.docCount;
                        totalStorageBytes += result.storageSize;
                        totalDataBytes += result.dataSize;
                        totalIndexBytes += result.indexSize;
                    }
                } catch (Exception e) {
                    logger.warn("dbStats failed for {}, using collection-level stats: {}", dbName, e.getMessage());
                    CollectionStatsResult result = getCollectionLevelStats(db);
                    totalDocuments += result.docCount;
                    totalCollections += result.collectionCount;
                    totalStorageBytes += result.storageSize;
                    totalDataBytes += result.dataSize;
                    totalIndexBytes += result.indexSize;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to fetch database list: {}", e.getMessage());
            throw new RuntimeException("Database connection error", e);
        }

        // Format results
        metrics.put("totalDatabases", totalDatabases);
        metrics.put("totalCollections", totalCollections);
        metrics.put("totalDocuments", totalDocuments);
        metrics.put("logicalDataSize", formatSize(totalDataBytes));
        metrics.put("storageSize", formatSize(totalStorageBytes));
        metrics.put("indexSize", formatSize(totalIndexBytes));
        metrics.put("compressionRatio", calculateCompressionRatio(totalDataBytes, totalStorageBytes));
        
        return metrics;
    }

    private CollectionStatsResult getCollectionLevelStats(MongoDatabase db) {
        CollectionStatsResult result = new CollectionStatsResult();
        
        // Get only user collections
        List<String> collectionNames = db.listCollectionNames()
            .into(new ArrayList<>())
            .stream()
            .filter(this::isUserCollection)
            .collect(Collectors.toList());
        
        result.collectionCount = collectionNames.size();

        for (String collName : collectionNames) {
            try {
                Document collStats = db.runCommand(new Document("collStats", collName));
                result.docCount += getSafeLong(collStats, "count");
                result.storageSize += getSafeLong(collStats, "storageSize");
                result.dataSize += getSafeLong(collStats, "size");
                result.indexSize += getSafeLong(collStats, "totalIndexSize");
            } catch (Exception e) {
                logger.warn("collStats failed for {}, using countDocuments: {}", collName, e.getMessage());
                try {
                    result.docCount += db.getCollection(collName).countDocuments();
                } catch (Exception ex) {
                    logger.error("countDocuments failed for {}: {}", collName, ex.getMessage());
                }
            }
        }
        return result;
    }

    private boolean isSystemDatabase(String dbName) {
        return dbName.equals("admin") || 
               dbName.equals("local") || 
               dbName.equals("config") ||
               dbName.startsWith("system.");
    }

    private boolean isUserCollection(String collectionName) {
        return !collectionName.startsWith("system.") && 
               !collectionName.equals("system.profile") &&
               !collectionName.equals("system.js") &&
               !collectionName.equals("system.views");
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " bytes";
        if (bytes < 1024 * 1024) return (bytes / 1024) + " KB";
        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private double calculateCompressionRatio(long logicalSize, long storageSize) {
        if (storageSize == 0) return 0;
        return Math.round((logicalSize / (double) storageSize) * 100) / 100.0;
    }

    private long getSafeLong(Document doc, String key) {
        try {
            Object value = doc.get(key);
            return (value instanceof Number) ? ((Number) value).longValue() : 0L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static class CollectionStatsResult {
        long docCount = 0;
        long storageSize = 0;
        long dataSize = 0;
        long indexSize = 0;
        int collectionCount = 0;
    }
}
