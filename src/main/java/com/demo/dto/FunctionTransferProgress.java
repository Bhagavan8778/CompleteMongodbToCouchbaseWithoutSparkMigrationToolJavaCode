//package com.demo.dto;
//
//import java.time.LocalDateTime;
//
//public class FunctionTransferProgress {
//    private String functionName;
//    private String status;
//    private int processed;
//    private int total;
//    private LocalDateTime timestamp;
//    private String message;
//
//    public FunctionTransferProgress(String functionName, String status, 
//                                  int processed, int total, String message) {
//        this.functionName = functionName;
//        this.status = status;
//        this.processed = processed;
//        this.total = total;
//        this.message = message;
//        this.timestamp = LocalDateTime.now();
//    }
//    
//    public String getFunctionName() { return functionName; }
//    public String getStatus() { return status; }
//    public int getProcessed() { return processed; }
//    public int getTotal() { return total; }
//    public LocalDateTime getTimestamp() { return timestamp; }
//    public String getMessage() { return message; }
//}


//package com.demo.dto;
//
//import java.time.LocalDateTime;
//import com.fasterxml.jackson.annotation.JsonFormat;
//import com.fasterxml.jackson.annotation.JsonInclude;
//
//@JsonInclude(JsonInclude.Include.NON_NULL)
//public class FunctionTransferProgress {
//    private String functionName;
//    private String status;
//    private int processed;
//    private int total;
//    
//    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
//    private LocalDateTime timestamp;
//    
//    private String message;
//
//    // Default constructor for JSON deserialization
//    public FunctionTransferProgress() {
//        this.timestamp = LocalDateTime.now();
//    }
//
//    public FunctionTransferProgress(String functionName, String status, 
//                                  int processed, int total, String message) {
//        this();
//        this.functionName = functionName;
//        this.status = status;
//        this.processed = processed;
//        this.total = total;
//        this.message = message;
//    }
//
//    // Getters
//    public String getFunctionName() { return functionName; }
//    public String getStatus() { return status; }
//    public int getProcessed() { return processed; }
//    public int getTotal() { return total; }
//    public LocalDateTime getTimestamp() { return timestamp; }
//    public String getMessage() { return message; }
//
//    // Setters (required for proper JSON deserialization)
//    public void setFunctionName(String functionName) { this.functionName = functionName; }
//    public void setStatus(String status) { this.status = status; }
//    public void setProcessed(int processed) { this.processed = processed; }
//    public void setTotal(int total) { this.total = total; }
//    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
//    public void setMessage(String message) { this.message = message; }
//
//    // Utility methods
//    public double getPercentage() {
//        return total > 0 ? (processed * 100.0) / total : 0;
//    }
//
//    public boolean isComplete() {
//        return "COMPLETED".equals(status);
//    }
//
//    @Override
//    public String toString() {
//        return String.format(
//            "FunctionProgress{function='%s', status=%s, %d/%d (%.1f%%)}",
//            functionName, status, processed, total, getPercentage()
//        );
//    }
//}

package com.demo.dto;

import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.*;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionTransferProgress {
    
    @JsonProperty("name")
    private String functionName;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("processed")
    private int processed;
    
    @JsonProperty("total")
    private int total;
    
    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    
    @JsonProperty("message")
    private String message;
    
    @JsonCreator
    public FunctionTransferProgress(
        @JsonProperty("name") String functionName,
        @JsonProperty("status") String status,
        @JsonProperty("processed") int processed,
        @JsonProperty("total") int total,
        @JsonProperty("message") String message) {
        
        this.functionName = functionName;
        this.status = status;
        this.processed = processed;
        this.total = total;
        this.message = message;
        this.timestamp = LocalDateTime.now();
    }

    // Simplified constructor for common cases
    public FunctionTransferProgress(String status, int processed, int total) {
        this(null, status, processed, total, null);
    }

    @JsonIgnore
    public double getPercentage() {
        return total > 0 ? Math.min(100, (processed * 100.0) / total) : 0;
    }

    @JsonIgnore
    public boolean isCompleted() {
        return "COMPLETED".equalsIgnoreCase(status);
    }

    @JsonIgnore
    public boolean isError() {
        return "ERROR".equalsIgnoreCase(status);
    }
}