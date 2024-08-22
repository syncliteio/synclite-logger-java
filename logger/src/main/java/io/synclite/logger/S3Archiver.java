package io.synclite.logger;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.AwsHostNameUtils;


public class S3Archiver extends FSArchiver {

	private boolean remoteWriteArchiveCreated;
	private String endPoint;
	private String dataStageBucketName;
	private String commandStageBucketName;
	private String accessKey;
	private String secretKey;

	AmazonS3 s3Client;

	S3Archiver(Path dbPath, String writeArchiveName, Path writeArchivePath, String readArchiveName, Path readArchivePath, String endPoint, String accessKey, String secretKey, String remoteDataDirectory, String remoteCommandDirectory, Logger tracer, Path encryptionKeyPath) {
		super(dbPath, writeArchiveName, writeArchivePath, readArchiveName, readArchivePath, tracer, encryptionKeyPath);
		this.remoteWriteArchiveCreated = false;
		this.endPoint= endPoint;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.dataStageBucketName = remoteDataDirectory;
		this.commandStageBucketName = remoteCommandDirectory;
		connect();
	}

	private final void connect() {
		try {
			AWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
			s3Client = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withEndpointConfiguration(new EndpointConfiguration (this.endPoint, AwsHostNameUtils.parseRegion(endPoint, null)))
					.build();

		} catch (Exception e) {
			tracer.error("Failed to create MinIO client : ", e);
		}
	}

	private final boolean isConnected() {
		return (this.s3Client != null);
	}

	/*
    @Override
    void moveToWriteArchive(Path sourceArtifactPath, String targetArtifactName) throws SQLException {
        Path localTargetPath = getTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
        if (sourceArtifactPath.toFile().exists()) {
            super.moveToWriteArchive(sourceArtifactPath, targetArtifactName);
        }
        try {
            if (localTargetPath.toFile().exists()) {
                if (!isConnected()) {
                    connect();
                }
                createWriteArchiveIfNotExists();
                s3Client.putObject(writeArchiveName, targetArtifactName, localTargetPath.toFile());
                localTargetPath.toFile().delete();
            } else {
            	throw new RemoteShippingException("Artifact " + localTargetPath + " is missing");
            }
        } catch (Exception e) {
            //Ignore exception. Network send of each artifact is retried perpetually
            throw new RemoteShippingException("Failed to ship artifact to remote destination with exception : ", e);
        }
    }

    @Override
    void copyToWriteArchive(Path sourceArtifactPath, String targetArtifactName) throws SQLException {
        Path localTargetPath = getTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
        if (sourceArtifactPath.toFile().exists()) {
            super.copyToWriteArchive(sourceArtifactPath, targetArtifactName);
        }
        try {
            if (localTargetPath.toFile().exists()) {
                if (!isConnected()) {
                    connect();
                }
            	createWriteArchiveIfNotExists();
                s3Client.putObject(writeArchiveName, targetArtifactName, localTargetPath.toFile());
           } else {
        	   throw new RemoteShippingException("Artifact " + localTargetPath + " is missing");
           }
        } catch (Exception e) {
            throw new RemoteShippingException("Failed to ship artifact : " + sourceArtifactPath + " to remote location", e);
        }
    }

    @Override
    void createWriteArchiveIfNotExists() throws SQLException {
    	if (this.remoteWriteArchiveCreated) {
            return;
        }
        super.createWriteArchiveIfNotExists();
        try {      	
        	this.remoteWriteArchiveCreated = s3Client.doesBucketExistV2(writeArchiveName);

        	if (!remoteWriteArchiveCreated) {
        		s3Client.createBucket(writeArchiveName);
        	}
        	this.remoteWriteArchiveCreated = s3Client.doesBucketExistV2(writeArchiveName);

        } catch(Exception e) {
            throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
        	//Ignore this exception as this is supposed to throw exception due to permission restrictions.
        }
    }

	 */   

	@Override
	void moveToWriteArchive(Path sourceArtifactPath, String targetArtifactName) throws SQLException {
		Path localTargetPath = getTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
		if (sourceArtifactPath.toFile().exists()) {
			super.moveToWriteArchive(sourceArtifactPath, targetArtifactName);
		}
		try {
			if (localTargetPath.toFile().exists()) {
				if (!isConnected()) {
					connect();
				}
				createWriteArchiveIfNotExists();
				try {
					s3Client.putObject(dataStageBucketName, writeArchiveName + "/" + targetArtifactName, localTargetPath.toFile());
				} catch (AmazonS3Exception e) {
					//If object already exists (uploaded earlier perhaps prior to a crash) then move on.
					if (e.getStatusCode() != 409) {
						throw e;
					}
				}
                try {
                	localTargetPath.toFile().delete();
                } catch (Exception e) {
                	//Ignore if we failed to delete the file from local stage directory.
                }
			} else {
				throw new RemoteShippingException("Artifact " + localTargetPath + " is missing");
			}
		} catch (Exception e) {
			//Ignore exception. Network send of each artifact is retried perpetually
			throw new RemoteShippingException("Failed to ship artifact to remote destination with exception : ", e);
		}
	}

	@Override
	void copyToWriteArchive(Path sourceArtifactPath, String targetArtifactName) throws SQLException {
		Path localTargetPath = getTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
		if (sourceArtifactPath.toFile().exists()) {
			super.copyToWriteArchive(sourceArtifactPath, targetArtifactName);
		}
		try {
			if (localTargetPath.toFile().exists()) {
				if (!isConnected()) {
					connect();
				}
				createWriteArchiveIfNotExists();
				try {
					//If object already exists (uploaded earlier perhaps prior to a crash) then move on.
					s3Client.putObject(dataStageBucketName, writeArchiveName + "/" + targetArtifactName, localTargetPath.toFile());
				} catch (AmazonS3Exception e) {
					if (e.getStatusCode() != 409) {
						throw e;
					}
				}
                try {
                	localTargetPath.toFile().delete();
                } catch (Exception e) {
                	//Ignore if we failed to delete the file from local stage directory.
                }
			} else {
				throw new RemoteShippingException("Artifact " + localTargetPath + " is missing");
			}
		} catch (Exception e) {
			throw new RemoteShippingException("Failed to ship artifact : " + sourceArtifactPath + " to remote location", e);
		}
	}

	@Override
	void createWriteArchiveIfNotExists() throws SQLException {
		if (this.remoteWriteArchiveCreated) {
			return;
		}
		super.createWriteArchiveIfNotExists();
		try {
			s3Client.putObject(dataStageBucketName, writeArchiveName + "/", new ByteArrayInputStream(new byte[0]), new ObjectMetadata());
			this.remoteWriteArchiveCreated = true;
		} catch (AmazonS3Exception e) {
			if (e.getStatusCode() == 409) {
				this.remoteWriteArchiveCreated = true;
			} else {
				throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
			}
		} catch(Exception e) {
			throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
		}
	}

	@Override
	List<Path> getObjectsInReadArchive() throws SQLException {    	
		try {
			//List<String> commandFiles = new ArrayList<String>();
			ListObjectsV2Result result = s3Client.listObjectsV2 (commandStageBucketName, readArchiveName + "/");
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {

				S3Object s3Object = s3Client.getObject(commandStageBucketName, objectSummary.getKey());

				String dstFilePath = readArchivePath.resolve(objectSummary.getKey()).toString();

				try (S3ObjectInputStream objectInputStream = s3Object.getObjectContent();
						OutputStream outputStream = new FileOutputStream(dstFilePath.toString())) {
					byte[] buffer = new byte[1024];
					int bytesRead;
					while ((bytesRead = objectInputStream.read(buffer)) != -1) {
						outputStream.write(buffer, 0, bytesRead);
					}
					outputStream.flush();
				}
				//commandFiles.add(objectSummary.getKey());
			}
			return super.getObjectsInReadArchive();
		}
		catch (Exception e) {
			throw new RemoteShippingException("Failed to lookup readArchive : " + readArchivePath + " from remote location : " + commandStageBucketName, e);
		}
	}

	@Override
	void terminate() {		
		//Nothing to do here.
	}
}
