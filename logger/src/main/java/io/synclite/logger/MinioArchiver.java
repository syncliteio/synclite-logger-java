package io.synclite.logger;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import io.minio.DownloadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.Result;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;

public class MinioArchiver extends FSArchiver {

    private boolean remoteWriteArchiveCreated;
    private String endPoint;
    private String accessKey;
    private String secretKey;
    private String dataStageBucketName;
    private String commandStageBucketName; 
   
    MinioArchiver(Path dbPath, String writeArchiveName, Path writeArchivePath, String readArchiveName, Path readArchivePath, String endPoint, String accessKey, String secretKey, String remoteDataDirectory, String remoteCommandDirectory, Logger tracer, Path encryptionKeyPath) {
        super(dbPath, writeArchiveName, writeArchivePath, readArchiveName, readArchivePath, tracer, encryptionKeyPath);
        this.remoteWriteArchiveCreated = false;
        this.endPoint= endPoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;   
        this.dataStageBucketName = remoteDataDirectory;
        this.commandStageBucketName = remoteCommandDirectory;
        connect();
    }

	MinioClient minioClient;

    private final boolean isConnected() {
    	return (this.minioClient != null);
    }

    @Override
    List<Path> getObjectsInReadArchive() throws SQLException {    	
    	try {
	    	//List<String> commandFiles = new ArrayList<String>();
	    	Iterable<Result<Item>> results =  minioClient.listObjects(ListObjectsArgs.builder().bucket(this.commandStageBucketName).prefix(readArchiveName + "/").build());
	    	for (Result<Item> result : results) {
	    		Item item = result.get();
            	String dstFilePath = readArchivePath.resolve(item.objectName()).toString();
            	String srcFilePath = readArchiveName + "/" + item.objectName();
            	
				minioClient.downloadObject(
						DownloadObjectArgs.builder()
						.bucket(this.commandStageBucketName)
						.object(srcFilePath)
						.filename(dstFilePath)
						.overwrite(true)
						.build());
	    		//commandFiles.add(item.objectName());
	    	}
	    	return super.getObjectsInReadArchive();
    	}
    	catch (Exception e) {
			throw new RemoteShippingException("Failed to lookup readArchive : " + readArchivePath + " from remote location : " + commandStageBucketName, e);
    	}
    }
    

	private final void connect() {
		try {
			this.minioClient =
					MinioClient.builder()
					.endpoint(this.endPoint)
					.credentials(this.accessKey, this.secretKey)
					.build();        	
		} catch (Exception e) {
			tracer.error("Failed to create MinIO client : ", e);
		}
	}


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
					minioClient.uploadObject(
							UploadObjectArgs.builder()
							.bucket(dataStageBucketName)
							.object(writeArchiveName + "/" + targetArtifactName)
							.filename(localTargetPath.toString())
							.build());
				} catch (ErrorResponseException e) {
					//If object already exists (uploaded earlier perhaps prior to a crash) then move on.
					if (e.response().code() != 409) {
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
					minioClient.uploadObject(
							UploadObjectArgs.builder()
							.bucket(dataStageBucketName)
							.object(writeArchiveName + "/" + targetArtifactName)
							.filename(localTargetPath.toString())
							.build());
				} catch (ErrorResponseException e) {
					//If object already exists (uploaded earlier perhaps prior to a crash) then move on.
					if (e.response().code() != 409) {
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
			PutObjectArgs putObjArgs = PutObjectArgs.builder()
					.bucket(dataStageBucketName)
					.object(writeArchiveName + "/")
					.stream(new ByteArrayInputStream(new byte[0]), 0, -1)
					.build();     	    		

			minioClient.putObject(putObjArgs);

			this.remoteWriteArchiveCreated = true;            
		} catch (ErrorResponseException e) {
			if (e.response().code() == 409) {
				this.remoteWriteArchiveCreated = true;
			} else {
				throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
			}
		} catch(Exception e) {
			throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
		}
	}

	@Override
	void terminate() {		
		//Nothing to do here.
	}
}
