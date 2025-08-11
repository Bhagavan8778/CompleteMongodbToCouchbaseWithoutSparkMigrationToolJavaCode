package com.demo.dto;
import jakarta.validation.constraints.NotBlank;
public class CouchbaseConnectionDetails {
	@NotBlank
    private String connectionString;
	@NotBlank
    private String username;
	@NotBlank
    private String password;
	
    private String bucketName;

	private byte[] certificate;
    public byte[] getCertificate() {
		return certificate;
	}

	public void setCertificate(byte[] certificate) {
		this.certificate = certificate;
	}

	public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}