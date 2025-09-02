package com.demo.service;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.demo.dto.CouchbaseConnectionDetails;
import com.demo.security.EncryptionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManagerFactory;

@Service
public class CouchbaseConnectionService {
    
    @Autowired
    private EncryptionService encryptionService;
    
    private Cluster cluster;
    private Bucket bucket;
    private byte[] currentCertificate;
    private CouchbaseConnectionDetails currentDetails;
    
    public byte[] getCurrentCertificate() {
        return currentCertificate;
    }
    
    public void storeCertificate(byte[] encryptedCertificate) {
        this.currentCertificate = encryptedCertificate;
    }
    
    public void initializeWithStoredCertificate(CouchbaseConnectionDetails details) throws Exception {
        if (currentCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        byte[] decryptedCert = encryptionService.decryptBytes(currentCertificate);
        initializeWithCertificate(details, decryptedCert);
    }
    
    public void initializeWithCertificate(
            CouchbaseConnectionDetails details,
            byte[] certificate
    ) throws Exception {
        this.currentDetails = details;
        
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        Certificate cert = CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(certificate));
        trustStore.setCertificateEntry("couchbase-cert", cert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        ClusterEnvironment env = ClusterEnvironment.builder()
            .securityConfig(SecurityConfig.enableTls(true).trustManagerFactory(tmf))
            .ioConfig(IoConfig.enableDnsSrv(true))
            .build();

        // Decrypt credentials if encrypted
        String username = decryptIfEncrypted(details.getUsername());
        String password = decryptIfEncrypted(details.getPassword());
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(username, password)
                .environment(env)
        );

        this.bucket = cluster.bucket(details.getBucketName());
    }

    public void initializeCluster(CouchbaseConnectionDetails details) throws Exception {
        if (currentCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        
        this.currentDetails = details;
        
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        Certificate cert = CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(currentCertificate));
        trustStore.setCertificateEntry("couchbase-cert", cert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        ClusterEnvironment env = ClusterEnvironment.builder()
            .securityConfig(SecurityConfig.enableTls(true).trustManagerFactory(tmf))
            .ioConfig(IoConfig.enableDnsSrv(true))
            .build();

        String username = decryptIfEncrypted(details.getUsername());
        String password = decryptIfEncrypted(details.getPassword());
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(username, password)
                .environment(env)
        );
    }

    public void initializeBucket(String bucketName) {
        this.bucket = cluster.bucket(bucketName);
    }

    public void initializeCouchbaseClient(CouchbaseConnectionDetails details) throws Exception {
        this.currentDetails = details;
        
        String username = decryptIfEncrypted(details.getUsername());
        String password = decryptIfEncrypted(details.getPassword());
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(username, password)
        );
        this.bucket = cluster.bucket(details.getBucketName());
    }

    public List<String> listAllBuckets() {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        return cluster.buckets().getAllBuckets().values().stream()
            .map(BucketSettings::name)
            .collect(Collectors.toList());
    }

    public Map<String, List<String>> listCouchbaseCollections() {
        if (bucket == null) {
            throw new IllegalStateException("Bucket not initialized");
        }
        Map<String, List<String>> scopeCollections = new HashMap<>();
        List<ScopeSpec> scopes = bucket.collections().getAllScopes();
        for (ScopeSpec scope : scopes) {
            List<String> collectionNames = scope.collections().stream()
                .map(CollectionSpec::name)
                .collect(Collectors.toList());
            scopeCollections.put(scope.name(), collectionNames);
        }
        return scopeCollections;
    }

    public Map<String, List<String>> getCollectionsForBucket(String bucketName) {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        Bucket tempBucket = cluster.bucket(bucketName);
        Map<String, List<String>> scopeCollections = new HashMap<>();
        
        List<ScopeSpec> scopes = tempBucket.collections().getAllScopes();
        for (ScopeSpec scope : scopes) {
            List<String> collectionNames = scope.collections().stream()
                .map(CollectionSpec::name)
                .collect(Collectors.toList());
            scopeCollections.put(scope.name(), collectionNames);
        }
        return scopeCollections;
    }

    public Collection getTargetCollection(String bucketName, String scopeName, String collectionName) {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        return cluster.bucket(bucketName)
            .scope(scopeName)
            .collection(collectionName);
    }

    public Bucket getBucket() {
        return bucket;
    }

    public Cluster getCluster() {
        return cluster;
    }
    
    public void ping() {
        if (this.cluster == null) {
            throw new IllegalStateException("Couchbase cluster is not initialized.");
        }
        try {
            this.cluster.ping();
        } catch (Exception ex) {
            throw new RuntimeException("Couchbase ping failed: " + ex.getMessage(), ex);
        }
    }
    
    public String getNodes() {
        if (cluster == null) {
            throw new IllegalStateException("Couchbase cluster is not initialized");
        }
        return currentDetails.getConnectionString();
    }
    
    public String getUsername() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getUsername();
    }
    
    public String getPassword() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getPassword();
    }
    
    public String getBucketName() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getBucketName();
    }
    
    public void closeConnection() {
        if (cluster != null) {
            cluster.disconnect();
            cluster = null;
        }
        bucket = null;
        currentDetails = null;
    }
    
    
    public void clearStoredCertificate() {
        if (currentCertificate != null) {
            // Securely wipe the byte array
            for (int i = 0; i < currentCertificate.length; i++) {
                currentCertificate[i] = 0;
            }
            currentCertificate = null;
        }
    }
    
    private String decryptIfEncrypted(String value) {
        if (value != null && encryptionService.isEncrypted(value)) {
            return encryptionService.decryptSensitive(value);
        }
        return value;
    }
    
   
    public boolean isConnected() {
        try {
            if (cluster != null) {
                cluster.ping();
                return true;
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }
   
    public Map<String, Object> getClusterInfo() {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        
        Map<String, Object> info = new HashMap<>();
        info.put("connected", isConnected());
        info.put("bucketCount", listAllBuckets().size());
        info.put("connectionString", currentDetails != null ? currentDetails.getConnectionString() : "N/A");
        info.put("username", currentDetails != null ? currentDetails.getUsername() : "N/A");
        
        return info;
    }
}