package com.demo.dto;

import java.time.LocalDateTime;

public class MigrationProgress {
    private String database;
    private String collection;
    private int transferred;
    private int total;
    private String status;
    private LocalDateTime timestamp;
    public MigrationProgress(String database, String collection, 
                           int transferred, int total, String status) {
        this.database = database;
        this.collection = collection;
        this.transferred = transferred;
        this.total = total;
        this.status = status;
        this.timestamp = LocalDateTime.now();
    }
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getCollection() {
		return collection;
	}
	public void setCollection(String collection) {
		this.collection = collection;
	}
	public int getTransferred() {
		return transferred;
	}
	public void setTransferred(int transferred) {
		this.transferred = transferred;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public LocalDateTime getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(LocalDateTime timestamp) {
		this.timestamp = timestamp;
	}
    
    
}