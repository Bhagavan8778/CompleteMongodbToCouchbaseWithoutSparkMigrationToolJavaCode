 
package com.demo.dto;
 
import java.util.ArrayList;
import java.util.List;
 
public class FunctionTransferRequest {
    private String mongoDatabase;
    private String couchbaseBucket;
    private String couchbaseScope;
    private boolean includeSystemFunctions;
    private List<String> functionNames = new ArrayList<>();
    private boolean continueOnError = false;
 
    public String getMongoDatabase() {
        return mongoDatabase;
    }
 
    public void setMongoDatabase(String mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }
 
    public String getCouchbaseBucket() {
        return couchbaseBucket;
    }
 
    public void setCouchbaseBucket(String couchbaseBucket) {
        this.couchbaseBucket = couchbaseBucket;
    }
 
    public String getCouchbaseScope() {
        return couchbaseScope;
    }
 
    public void setCouchbaseScope(String couchbaseScope) {
        this.couchbaseScope = couchbaseScope;
    }
 
    public boolean isIncludeSystemFunctions() {
        return includeSystemFunctions;
    }
 
    public void setIncludeSystemFunctions(boolean includeSystemFunctions) {
        this.includeSystemFunctions = includeSystemFunctions;
    }
 
    public List<String> getFunctionNames() {
        return functionNames;
    }
 
    public void setFunctionNames(List<String> functionNames) {
        this.functionNames = functionNames != null ? functionNames : new ArrayList<>();
    }
 
    public boolean isContinueOnError() {
        return continueOnError;
    }
 
    public void setContinueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }
}
 