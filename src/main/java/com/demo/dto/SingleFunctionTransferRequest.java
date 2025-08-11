 
package com.demo.dto;
 
public class SingleFunctionTransferRequest {
    private String mongoDatabase;
    private String functionName;
    private String couchbaseScope;
 
    public String getMongoDatabase() {
        return mongoDatabase;
    }
 
    public void setMongoDatabase(String mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }
 
    public String getFunctionName() {
        return functionName;
    }
 
    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }
 
    public String getCouchbaseScope() {
        return couchbaseScope;
    }
 
    public void setCouchbaseScope(String couchbaseScope) {
        this.couchbaseScope = couchbaseScope;
    }
}
 