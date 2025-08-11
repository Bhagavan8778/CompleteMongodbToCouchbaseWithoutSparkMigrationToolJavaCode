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
    
    private Cluster cluster;
    private Bucket bucket;
    private byte[] currentCertificate;
 
    public void storeCertificate(byte[] certificate) {
        this.currentCertificate = certificate;
    }
    
    public void initializeWithStoredCertificate(CouchbaseConnectionDetails details) throws Exception {
        if (currentCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        initializeWithCertificate(details, currentCertificate);
    }
    
    public void initializeWithCertificate(
            CouchbaseConnectionDetails details,
            byte[] certificate
    ) throws Exception {
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
 
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(details.getUsername(), details.getPassword())
                .environment(env)
        );
 
        this.bucket = cluster.bucket(details.getBucketName());
    }
 
    public List<String> listBuckets() {
        return cluster.buckets().getAllBuckets().keySet().stream().toList();
    }
    
    public void initializeCluster(CouchbaseConnectionDetails details) throws Exception {
        if (currentCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        
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
 
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(details.getUsername(), details.getPassword())
                .environment(env)
        );
    }
 
    public void initializeBucket(String bucketName) {
        this.bucket = cluster.bucket(bucketName);
    }
 
    public List<String> listAllBuckets() {
        return cluster.buckets().getAllBuckets().values().stream()
            .map(BucketSettings::name)
            .collect(Collectors.toList());
    }
 
  public void initializeCouchbaseClient(CouchbaseConnectionDetails details) throws Exception {
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(details.getUsername(), details.getPassword())
        );
        this.bucket = cluster.bucket(details.getBucketName());
    }
  
  public Map<String, List<String>> listCouchbaseCollections() {
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
  
    // New dynamic bucket exploration method
    public Map<String, List<String>> getCollectionsForBucket(String bucketName) {
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
    
}
	
    
    
    
 
//package com.demo.service;
//
//import com.couchbase.client.java.Bucket;
//
//import com.couchbase.client.java.Cluster;
//import com.couchbase.client.java.ClusterOptions;
//import com.couchbase.client.java.env.ClusterEnvironment;
//import com.couchbase.client.java.manager.collection.CollectionSpec;
//import com.couchbase.client.java.manager.collection.ScopeSpec;
//import com.couchbase.client.core.env.IoConfig;
//import com.couchbase.client.core.env.SecurityConfig;
//import com.demo.dto.CouchbaseConnectionDetails;
//import org.springframework.stereotype.Service;
//
//import java.io.ByteArrayInputStream;
//import java.security.KeyStore;
//import java.security.cert.Certificate;
//import java.security.cert.CertificateFactory;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import javax.net.ssl.TrustManagerFactory;
//
//@Service
//public class CouchbaseConnectionService {
//    
//    private Cluster cluster;
//    private Bucket bucket;
//    private byte[] currentCertificate;
//
//    public void storeCertificate(byte[] certificate) {
//        this.currentCertificate = certificate;
//    }
//    
//    public void initializeWithStoredCertificate(CouchbaseConnectionDetails details) throws Exception {
//        if (currentCertificate == null) {
//            throw new IllegalStateException("No certificate stored");
//        }
//        initializeWithCertificate(details, currentCertificate);
//    }
//
//    public void initializeWithCertificate(
//            CouchbaseConnectionDetails details,
//            byte[] certificate
//    ) throws Exception {
//        KeyStore trustStore = KeyStore.getInstance("JKS");
//        trustStore.load(null, null);
//        Certificate cert = CertificateFactory.getInstance("X.509")
//            .generateCertificate(new ByteArrayInputStream(certificate));
//        trustStore.setCertificateEntry("couchbase-cert", cert);
//
//        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
//        tmf.init(trustStore);
//
//        ClusterEnvironment env = ClusterEnvironment.builder()
//            .securityConfig(SecurityConfig.enableTls(true).trustManagerFactory(tmf))
//            .ioConfig(IoConfig.enableDnsSrv(true))
//            .build();
//
//        
//        this.cluster = Cluster.connect(
//            details.getConnectionString(),
//            ClusterOptions.clusterOptions(details.getUsername(), details.getPassword())
//                .environment(env)
//        );
//
//        this.bucket = cluster.bucket(details.getBucketName());
//    }
//
//    public List<String> listBuckets() {
//        return cluster.buckets().getAllBuckets().keySet().stream().toList();
//    }
//
//    public Map<String, List<String>> listCouchbaseCollections() {
//        Map<String, List<String>> scopeCollections = new HashMap<>();
//        List<ScopeSpec> scopes = bucket.collections().getAllScopes();
//        for (ScopeSpec scope : scopes) {
//            List<String> collectionNames = scope.collections().stream()
//                .map(CollectionSpec::name)
//                .collect(Collectors.toList());
//            scopeCollections.put(scope.name(), collectionNames);
//        }
//        return scopeCollections;
//    }
//    public void initializeCouchbaseClient(CouchbaseConnectionDetails details) throws Exception {
//        
//        this.cluster = Cluster.connect(
//            details.getConnectionString(),
//            ClusterOptions.clusterOptions(details.getUsername(), details.getPassword())
//        );
//        this.bucket = cluster.bucket(details.getBucketName());
//    }
//    public Bucket getBucket() {
//        return bucket;
//    }
//}
    
 
 