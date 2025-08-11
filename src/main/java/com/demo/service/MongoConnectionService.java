package com.demo.service;

import com.demo.dto.MongoConnectionDetails;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import org.apache.http.ssl.SSLContexts;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

@Service
public class MongoConnectionService {

    private MongoClient mongoClient;
    private byte[] storedCertificate;
    public void storeCertificate(byte[] certificate) {
        this.storedCertificate = certificate;
    }

    public void initializeWithStoredCertificate(MongoConnectionDetails details) throws Exception {
        if (storedCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        initializeWithCertificate(details.getUri(), storedCertificate);
    }
    

    public void initializeMongoClient(MongoConnectionDetails details) {
        if (details.getUri() != null && !details.getUri().isEmpty()) {
            this.mongoClient = MongoClients.create(details.getUri());
        } else {
            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                    builder.hosts(List.of(new ServerAddress(details.getHost(), details.getPort())))
                );

            if (details.getUsername() != null && details.getPassword() != null) {
                MongoCredential credential = MongoCredential.createCredential(
                    details.getUsername(),
                    details.getDatabase(),
                    details.getPassword().toCharArray()
                );
                settingsBuilder.credential(credential);
            }

            MongoClientSettings settings = settingsBuilder.build();
            this.mongoClient = MongoClients.create(settings);
        }
    }
    public void initializeWithCertificate(String connectionString, byte[] certificate) 
            throws Exception {

    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, null);
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    Certificate cert = cf.generateCertificate(new ByteArrayInputStream(certificate));
    trustStore.setCertificateEntry("mongo-atlas", cert);
 
    SSLContext sslContext = SSLContexts.custom()
        .loadTrustMaterial(trustStore, null)
        .build();
   
    this.mongoClient = MongoClients.create(
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(connectionString))
            .applyToSslSettings(builder -> 
                builder.enabled(true)
                       .context(sslContext)
            )
            .build()
    );
}

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public List<String> listDatabases() {
        List<String> dbNames = new ArrayList<>();
        mongoClient.listDatabaseNames().forEach(dbNames::add);
        return dbNames;
    }

    public List<String> listCollectionsInDatabase(String dbName) {
        return mongoClient.getDatabase(dbName).listCollectionNames().into(new ArrayList<>());
    }
}