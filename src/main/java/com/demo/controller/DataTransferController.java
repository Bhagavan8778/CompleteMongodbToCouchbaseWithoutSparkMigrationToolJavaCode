//package com.demo.controller;
// 
//import com.demo.dto.CouchbaseConnectionDetails;
//import com.demo.dto.ErrorResponse;
//import com.demo.dto.FunctionTransferRequest;
//import com.demo.dto.MongoConnectionDetails;
//import com.demo.dto.SingleFunctionTransferRequest;
//import com.demo.kafka.KafkaProducer;
//import com.demo.service.*;
//import com.mongodb.client.MongoClient;
//import com.demo.config.TemporaryConnectionCache;
//import jakarta.validation.Valid;
// 
//import org.apache.http.HttpStatus;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.ResponseEntity;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.security.oauth2.jwt.Jwt;
//import org.springframework.security.oauth2.jwt.JwtDecoder;
//import org.springframework.web.bind.annotation.*;
//import org.springframework.web.multipart.MultipartFile;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
// 
//@RestController
//@RequestMapping("/api/transfer")
//public class DataTransferController {
// 
//    @Autowired
//    private MongoConnectionService mongoConnectionService;
//    
//    @Autowired
//    private CouchbaseConnectionService couchbaseConnectionService;
//    
//    @Autowired
//    private DataTransferService dataTransferService;
//    
//    @Autowired
//    private JwtDecoder jwtDecoder;
//    
//    @Autowired
//    private FunctionTransferService functionTransferService;
//     
//    @Autowired
//    private MongoFunctionService mongoFunctionService;
// 
//    @Autowired
//    private TemporaryConnectionCache connectionCache;
//    @Autowired
//    private MongoMetadataService mongoMetadataService;
//    @Autowired
//    private KafkaProducer kafkaProducer;
// 
//    @PostMapping("/upload/mongo-certificate")
//    public ResponseEntity<String> uploadMongoCertificate(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestParam("certificate") MultipartFile certificate) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            mongoConnectionService.storeCertificate(certificate.getBytes());
//            return ResponseEntity.ok("MongoDB certificate stored successfully");
//        } catch (IOException e) {
//            return ResponseEntity.badRequest().body("Failed to store certificate: " + e.getMessage());
//        }
//    }
// 
//    @PostMapping("/upload/couchbase-certificate")
//    public ResponseEntity<String> uploadCouchbaseCertificate(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestParam("certificate") MultipartFile certificate) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            couchbaseConnectionService.storeCertificate(certificate.getBytes());
//            return ResponseEntity.ok("Couchbase certificate stored successfully");
//        } catch (IOException e) {
//            return ResponseEntity.badRequest().body("Failed to store certificate: " + e.getMessage());
//        }
//    }
// 
//    @PostMapping("/connect-mongo")
//    public ResponseEntity<String> connectToMongo(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestBody @Valid MongoConnectionDetails details) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            connectionCache.store(userId, "mongo", details);
//            mongoConnectionService.initializeMongoClient(details);
//            return ResponseEntity.ok("MongoDB connection initialized successfully.");
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError()
//                    .body("Failed to connect to MongoDB: " + e.getMessage());
//        }
//    }
//    @PostMapping("/connect-couchbase")
//    public ResponseEntity<String> connectToCouchbase(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestBody @Valid CouchbaseConnectionDetails details) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            connectionCache.store(userId, "couchbase", details);
//            couchbaseConnectionService.initializeWithStoredCertificate(details);
//            return ResponseEntity.ok("Couchbase connection initialized successfully.");
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError()
//                    .body("Failed to connect to Couchbase: " + e.getMessage());
//        }
//    }
// 
// 
//    @PostMapping("/select-bucket")
//    public ResponseEntity<?> selectBucket(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestParam String bucketName) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
//            if (details == null) {
//                return ResponseEntity.badRequest().body(Map.of(
//                    "status", "ERROR",
//                    "message", "No active Couchbase connection"
//                ));
//            }
//            
//            // Update with selected bucket
//            details.setBucketName(bucketName);
//            connectionCache.store(userId, "couchbase", details);
//            couchbaseConnectionService.initializeBucket(bucketName);
//            Map<String, List<String>> scopeCollections =
//                couchbaseConnectionService.listCouchbaseCollections();
//            return ResponseEntity.ok(Map.of(
//                "status", "SUCCESS",
//                "selectedBucket", bucketName,
//                "scopes", scopeCollections
//            ));
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(Map.of(
//                "status", "ERROR",
//                "message", "Failed to select bucket: " + e.getMessage()
//            ));
//        }
//    }
////
////    @PostMapping("/connect-couchbase")
////    public ResponseEntity<String> connectToCouchbase(
////            @RequestHeader("Authorization") String authHeader,
////            @RequestBody @Valid CouchbaseConnectionDetails details) {
////        try {
////            String userId = extractUserIdFromToken(authHeader);
////            connectionCache.store(userId, "couchbase", details);
////            couchbaseConnectionService.initializeWithStoredCertificate(details);
////            return ResponseEntity.ok("Couchbase connection initialized successfully.");
////        } catch (Exception e) {
////            return ResponseEntity.internalServerError()
////                    .body("Failed to connect to Couchbase: " + e.getMessage());
////        }
////    }
// 
//    @GetMapping("/databases")
//    public ResponseEntity<List<String>> getDatabases(
//            @RequestHeader("Authorization") String authHeader) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
//            if (details == null) {
//                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
//            }
//            return ResponseEntity.ok(mongoConnectionService.listDatabases());
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(List.of("Error listing databases: " + e.getMessage()));
//        }
//    }
// 
//    @GetMapping("/databases/{db}/collections")
//    public ResponseEntity<List<String>> getSourceCollections(
//            @RequestHeader("Authorization") String authHeader,
//            @PathVariable String db) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
//            if (details == null) {
//                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
//            }
//            return ResponseEntity.ok(mongoConnectionService.listCollectionsInDatabase(db));
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(List.of("Error listing collections: " + e.getMessage()));
//        }
//    }
//    @GetMapping("/buckets")
//    public ResponseEntity<?> listBuckets(@RequestHeader("Authorization") String authHeader) {
//        return ResponseEntity.ok(Map.of(
//            "buckets", couchbaseConnectionService.listAllBuckets()
//        ));
//    }
// 
//    @GetMapping("/{bucketName}/scopes")
//    public ResponseEntity<?> getBucketStructure(
//            @RequestHeader("Authorization") String authHeader,
//            @PathVariable String bucketName) {
//        return ResponseEntity.ok(
//            couchbaseConnectionService.getCollectionsForBucket(bucketName)
//        );
//    }
//    @GetMapping("/couchbase-collections")
//    public ResponseEntity<Map<String, List<String>>> getCouchbaseCollections(
//            @RequestHeader("Authorization") String authHeader) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
//            if (details == null) {
//                return ResponseEntity.badRequest().body(Map.of("error", List.of("No active Couchbase connection")));
//            }
//            return ResponseEntity.ok(couchbaseConnectionService.listCouchbaseCollections());
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body(Map.of("error", List.of("Error listing collections: " + e.getMessage())));
//        }
//    }
//    @PostMapping("/transfer")
//    public ResponseEntity<String> transferData(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestBody DataTransferService.TransferRequest request) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            dataTransferService.transferCollection(request);
//            return ResponseEntity.ok("Transfer initiated successfully");
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError()
//                .body("Transfer failed: " + e.getMessage());
//        }
//    }
////    @PostMapping("/transfer")
////    public ResponseEntity<String> transferCollections(
////            @RequestHeader("Authorization") String authHeader,
////            @RequestParam String db,
////            @RequestParam(required = false) String couchbaseScope,
////            @RequestBody Map<String, String> collectionsMap) {
////        try {
////            String userId = extractUserIdFromToken(authHeader);
////            MongoConnectionDetails mongoDetails = connectionCache.getMongoConnection(userId);
////            CouchbaseConnectionDetails couchbaseDetails = connectionCache.getCouchbaseConnection(userId);
////            if (mongoDetails == null || couchbaseDetails == null) {
////                return ResponseEntity.badRequest().body("Both MongoDB and Couchbase connections must be established");
////            }
////            dataTransferService.transferMultipleCollections(db, collectionsMap, couchbaseScope);
////            return ResponseEntity.ok("Transfer of " + collectionsMap.size() + " collections completed from DB: " + db);
////        } catch (Exception e) {
////            return ResponseEntity.internalServerError().body("Transfer failed: " + e.getMessage());
////        }
////    }
//    
//    private String extractUserIdFromToken(String authHeader) {
//        String token = authHeader.substring(7);
//        Jwt jwt = jwtDecoder.decode(token);
//       
//        return jwt.getClaim("sub");
//    }
// 
//@GetMapping("/metadata/mongo")
//public ResponseEntity<?> getMongoMetadata(@RequestHeader("Authorization") String authHeader) {
//   try {
//        String userId = extractUserIdFromToken(authHeader);
//        MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
//        if (details == null) {
//            return ResponseEntity.badRequest().body("No active MongoDB connection");
//        }
//        
//        List<Map<String, Object>> metadata = mongoMetadataService.getDatabaseMetadata();
//        kafkaProducer.sendMetadata(metadata);
//        
//       return ResponseEntity.ok(metadata);
//    } catch (Exception e) {
//        return ResponseEntity.internalServerError()
//                .body("Failed to get metadata: " + e.getMessage());
//    }
//}
// 
////    @GetMapping("/mongo/metadata")
////    public ResponseEntity<String> sendMongoMetadata(@RequestHeader("Authorization") String authHeader) {
////        try {
////            String userId = extractUserIdFromToken(authHeader);
////            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
////            if (details == null) {
////                return ResponseEntity.badRequest().body("No active MongoDB connection found.");
////            }
////
////            List<Map<String, Object>> metadata = mongoMetadataService.getDatabaseMetadata();
////            kafkaProducer.sendMetadata(metadata);
////            return ResponseEntity.ok("MongoDB metadata sent to Kafka.");
////        } catch (Exception e) {
////            return ResponseEntity.internalServerError().body("Failed to send metadata: " + e.getMessage());
////        }
////    }
//    private final List<String> kafkaMessages = new ArrayList<>();
// 
//    @KafkaListener(topics = "mongo-metadata", groupId = "spring-boot-consumer")
//    public void listen(String message) {
//        kafkaMessages.add(message);
//   }
// 
//    @GetMapping("/api/kafka/messages")
//    public List<String> getMessages() {
//        return kafkaMessages;
//    }
//    
//    @PostMapping("/transfer-functions")
//    public ResponseEntity<?> transferFunctions(@RequestBody FunctionTransferRequest request) {
//        try {
//            functionTransferService.transferAllFunctions(request);
//            return ResponseEntity.ok("Functions transferred successfully");
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
//                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
//        }
//    }
// 
//    @PostMapping("/transfer-function")
//    public ResponseEntity<?> transferSingleFunction(@RequestBody SingleFunctionTransferRequest request) {
//        try {
//            boolean success = functionTransferService.transferSingleFunction(
//                request.getMongoDatabase(),
//                request.getFunctionName(),
//                request.getCouchbaseScope()
//            );
//            
//            if (success) {
//                return ResponseEntity.ok("Function '" + request.getFunctionName() + "' transferred successfully");
//            } else {
//                return ResponseEntity.status(HttpStatus.SC_NOT_FOUND)
//                    .body(new ErrorResponse("Function not found",
//                        "Function '" + request.getFunctionName() + "' not found in database " + request.getMongoDatabase(), null));
//            }
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
//                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
//        }
//    }
// 
//    @GetMapping("/mongo-functions")
//    public ResponseEntity<?> getMongoFunctions(
//            @RequestParam String database,
//            @RequestParam(required = false, defaultValue = "false") boolean includeSystem) {
//        try {
//            List<org.bson.Document> functions = mongoFunctionService.getAllFunctions(database, includeSystem);
//            return ResponseEntity.ok(functions);
//        } catch (Exception e) {
//            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
//                    .body(new ErrorResponse("Error retrieving functions", e.getMessage(), database));
//        }
//    }
//    
//  
//}


package com.demo.controller;
 
import com.demo.dto.CouchbaseConnectionDetails;

import com.demo.dto.ErrorResponse;
import com.demo.dto.FunctionTransferRequest;
import com.demo.dto.MongoConnectionDetails;
import com.demo.dto.SingleFunctionTransferRequest;
import com.demo.kafka.KafkaProducer;
import com.demo.service.*;
import com.mongodb.client.MongoClient;
import com.demo.config.TemporaryConnectionCache;
import jakarta.validation.Valid;
 
import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
 
@RestController
@RequestMapping("/api/transfer")
public class DataTransferController {
 
    @Autowired
    private MongoConnectionService mongoConnectionService;
    
    @Autowired
    private CouchbaseConnectionService couchbaseConnectionService;
    
    @Autowired
    private DataTransferService dataTransferService;
    
    @Autowired
    private JwtDecoder jwtDecoder;
    
    @Autowired
    private FunctionTransferService functionTransferService;
     
    @Autowired
    private MongoFunctionService mongoFunctionService;
 
    @Autowired
    private TemporaryConnectionCache connectionCache;
    @Autowired
    private MongoMetadataService mongoMetadataService;
    @Autowired
    private KafkaProducer kafkaProducer;
 
    @PostMapping("/upload/mongo-certificate")
    public ResponseEntity<String> uploadMongoCertificate(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam("certificate") MultipartFile certificate) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            mongoConnectionService.storeCertificate(certificate.getBytes());
            return ResponseEntity.ok("MongoDB certificate stored successfully");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("Failed to store certificate: " + e.getMessage());
        }
    }
 
    @PostMapping("/upload/couchbase-certificate")
    public ResponseEntity<String> uploadCouchbaseCertificate(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam("certificate") MultipartFile certificate) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            couchbaseConnectionService.storeCertificate(certificate.getBytes());
            return ResponseEntity.ok("Couchbase certificate stored successfully");
        } catch (IOException e) {
            return ResponseEntity.badRequest().body("Failed to store certificate: " + e.getMessage());
        }
    }
 
    @PostMapping("/connect-mongo")
    public ResponseEntity<String> connectToMongo(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody @Valid MongoConnectionDetails details) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            connectionCache.store(userId, "mongo", details);
            mongoConnectionService.initializeMongoClient(details);
            return ResponseEntity.ok("MongoDB connection initialized successfully.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body("Failed to connect to MongoDB: " + e.getMessage());
        }
    }
    @PostMapping("/connect-couchbase")
    public ResponseEntity<String> connectToCouchbase(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody @Valid CouchbaseConnectionDetails details) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            connectionCache.store(userId, "couchbase", details);
            couchbaseConnectionService.initializeWithStoredCertificate(details);
            return ResponseEntity.ok("Couchbase connection initialized successfully.");
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body("Failed to connect to Couchbase: " + e.getMessage());
        }
    }
 
 
    @PostMapping("/select-bucket")
    public ResponseEntity<?> selectBucket(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam String bucketName) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
            if (details == null) {
                return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "No active Couchbase connection"
                ));
            }
            
            // Update with selected bucket
            details.setBucketName(bucketName);
            connectionCache.store(userId, "couchbase", details);
            couchbaseConnectionService.initializeBucket(bucketName);
            Map<String, List<String>> scopeCollections =
                couchbaseConnectionService.listCouchbaseCollections();
            return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "selectedBucket", bucketName,
                "scopes", scopeCollections
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "ERROR",
                "message", "Failed to select bucket: " + e.getMessage()
            ));
        }
    }
//
//    @PostMapping("/connect-couchbase")
//    public ResponseEntity<String> connectToCouchbase(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestBody @Valid CouchbaseConnectionDetails details) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            connectionCache.store(userId, "couchbase", details);
//            couchbaseConnectionService.initializeWithStoredCertificate(details);
//            return ResponseEntity.ok("Couchbase connection initialized successfully.");
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError()
//                    .body("Failed to connect to Couchbase: " + e.getMessage());
//        }
//    }
 
    @GetMapping("/databases")
    public ResponseEntity<List<String>> getDatabases(
            @RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
            if (details == null) {
                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
            }
            return ResponseEntity.ok(mongoConnectionService.listDatabases());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(List.of("Error listing databases: " + e.getMessage()));
        }
    }
 
    @GetMapping("/databases/{db}/collections")
    public ResponseEntity<List<String>> getSourceCollections(
            @RequestHeader("Authorization") String authHeader,
            @PathVariable String db) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
            if (details == null) {
                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
            }
            return ResponseEntity.ok(mongoConnectionService.listCollectionsInDatabase(db));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(List.of("Error listing collections: " + e.getMessage()));
        }
    }
    @GetMapping("/buckets")
    public ResponseEntity<?> listBuckets(@RequestHeader("Authorization") String authHeader) {
        return ResponseEntity.ok(Map.of(
            "buckets", couchbaseConnectionService.listAllBuckets()
        ));
    }
 
    @GetMapping("/{bucketName}/scopes")
    public ResponseEntity<?> getBucketStructure(
            @RequestHeader("Authorization") String authHeader,
            @PathVariable String bucketName) {
        return ResponseEntity.ok(
            couchbaseConnectionService.getCollectionsForBucket(bucketName)
        );
    }
    @GetMapping("/couchbase-collections")
    public ResponseEntity<Map<String, List<String>>> getCouchbaseCollections(
            @RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
            if (details == null) {
                return ResponseEntity.badRequest().body(Map.of("error", List.of("No active Couchbase connection")));
            }
            return ResponseEntity.ok(couchbaseConnectionService.listCouchbaseCollections());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", List.of("Error listing collections: " + e.getMessage())));
        }
    }
    @PostMapping("/transfer")
    public ResponseEntity<String> transferData(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody DataTransferService.TransferRequest request) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            dataTransferService.transferCollection(request);
            return ResponseEntity.ok("Transfer initiated successfully");
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body("Transfer failed: " + e.getMessage());
        }
    }
//    @PostMapping("/transfer")
//    public ResponseEntity<String> transferCollections(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestParam String db,
//            @RequestParam(required = false) String couchbaseScope,
//            @RequestBody Map<String, String> collectionsMap) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            MongoConnectionDetails mongoDetails = connectionCache.getMongoConnection(userId);
//            CouchbaseConnectionDetails couchbaseDetails = connectionCache.getCouchbaseConnection(userId);
//            if (mongoDetails == null || couchbaseDetails == null) {
//                return ResponseEntity.badRequest().body("Both MongoDB and Couchbase connections must be established");
//            }
//            dataTransferService.transferMultipleCollections(db, collectionsMap, couchbaseScope);
//            return ResponseEntity.ok("Transfer of " + collectionsMap.size() + " collections completed from DB: " + db);
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body("Transfer failed: " + e.getMessage());
//        }
//    }
    
    private String extractUserIdFromToken(String authHeader) {
        String token = authHeader.substring(7);
        Jwt jwt = jwtDecoder.decode(token);
       
        return jwt.getClaim("sub");
    }
 
@GetMapping("/metadata/mongo")
public ResponseEntity<?> getMongoMetadata(@RequestHeader("Authorization") String authHeader) {
   try {
        String userId = extractUserIdFromToken(authHeader);
        MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
        if (details == null) {
            return ResponseEntity.badRequest().body("No active MongoDB connection");
        }
        
//        List<Map<String, Object>> metadata = mongoMetadataService.getTotalStats();
        Map<String, Object> stats = mongoMetadataService.getStorageMetrics();
//        kafkaProducer.sendMetadata(metadata);
//        
//       return ResponseEntity.ok(metadata);
        kafkaProducer.sendMetadata(stats);
        return ResponseEntity.ok(stats);
    } catch (Exception e) {
        return ResponseEntity.internalServerError()
                .body("Failed to get metadata: " + e.getMessage());
    }
}
 
//    @GetMapping("/mongo/metadata")
//    public ResponseEntity<String> sendMongoMetadata(@RequestHeader("Authorization") String authHeader) {
//        try {
//            String userId = extractUserIdFromToken(authHeader);
//            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
//            if (details == null) {
//                return ResponseEntity.badRequest().body("No active MongoDB connection found.");
//            }
//
//            List<Map<String, Object>> metadata = mongoMetadataService.getDatabaseMetadata();
//            kafkaProducer.sendMetadata(metadata);
//            return ResponseEntity.ok("MongoDB metadata sent to Kafka.");
//        } catch (Exception e) {
//            return ResponseEntity.internalServerError().body("Failed to send metadata: " + e.getMessage());
//        }
//    }
    private final List<String> kafkaMessages = new ArrayList<>();
 
    @KafkaListener(topics = "mongo-metadata", groupId = "spring-boot-consumer")
    public void listen(String message) {
        kafkaMessages.add(message);
   }
 
    @GetMapping("/api/kafka/messages")
    public List<String> getMessages() {
        return kafkaMessages;
    }
    
    @PostMapping("/transfer-functions")
    public ResponseEntity<?> transferFunctions(@RequestBody FunctionTransferRequest request) {
        try {
            functionTransferService.transferAllFunctions(request);
            return ResponseEntity.ok("Functions transferred successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
        }
    }
 
    @PostMapping("/transfer-function")
    public ResponseEntity<?> transferSingleFunction(@RequestBody SingleFunctionTransferRequest request) {
        try {
            boolean success = functionTransferService.transferSingleFunction(
                request.getMongoDatabase(),
                request.getFunctionName(),
                request.getCouchbaseScope()
            );
            
            if (success) {
                return ResponseEntity.ok("Function '" + request.getFunctionName() + "' transferred successfully");
            } else {
                return ResponseEntity.status(HttpStatus.SC_NOT_FOUND)
                    .body(new ErrorResponse("Function not found",
                        "Function '" + request.getFunctionName() + "' not found in database " + request.getMongoDatabase(), null));
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
        }
    }
 
    @GetMapping("/mongo-functions")
    public ResponseEntity<?> getMongoFunctions(
            @RequestParam String database,
            @RequestParam(required = false, defaultValue = "false") boolean includeSystem) {
        try {
            List<org.bson.Document> functions = mongoFunctionService.getAllFunctions(database, includeSystem);
            return ResponseEntity.ok(functions);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Error retrieving functions", e.getMessage(), database));
        }
    }
    
  
}