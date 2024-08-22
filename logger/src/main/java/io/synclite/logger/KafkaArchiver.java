/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package io.synclite.logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class KafkaArchiver extends FSArchiver {

	private static final int FILE_CHUNK_SIZE = (1024*1024) - 1024;
	private final HashMap<String, String> kafkaProducerProperties;
	private final HashMap<String, String> kafkaConsumerProperties;
	private KafkaProducer<String, Map> producer;
	private KafkaConsumer<String, String> consumer;
	private boolean remoteWriteArchiveCreated;
    private final String producerTopicName;
    private final String consumerTopicName;

	KafkaArchiver(Path dbPath, String writArchiveName, Path writeArchivePath, String readArchiveName, Path readArchivePath, HashMap<String, String> kafkaProducerProperties, HashMap<String, String> kafkaConsumerProperties, String topicSuffix, Logger tracer, Path encryptionKeyPath) {
		super(dbPath, writArchiveName, writeArchivePath, readArchiveName, readArchivePath, tracer, encryptionKeyPath);
		this.kafkaProducerProperties = kafkaProducerProperties;
		this.kafkaConsumerProperties = kafkaConsumerProperties;
		this.remoteWriteArchiveCreated = false;
		this.producerTopicName = writeArchiveName + "-" + topicSuffix;
		this.consumerTopicName = readArchiveName + "-" + topicSuffix;
		connect();
	}
	
	private Properties getProducerProperties() {
		Properties props = new Properties();
		
		for (Map.Entry<String, String> kafkaProp : kafkaProducerProperties.entrySet()) {
			props.put(kafkaProp.getKey(), kafkaProp.getValue());
		}
		props.put(ProducerConfig.CLIENT_ID_CONFIG, writeArchiveName);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				SyncLiteFileSerializer.class.getName());		
		return props;
	}

	private Properties getConsumerProperties() {
		Properties props = new Properties();
		
		for (Map.Entry<String, String> kafkaProp : kafkaConsumerProperties.entrySet()) {
			props.put(kafkaProp.getKey(), kafkaProp.getValue());
		}
		return props;
	}

	private KafkaProducer<String,Map> createProducer() {
		return new KafkaProducer<String, Map>(getProducerProperties());
	}

	private KafkaConsumer<String, String> createConsumer() {
		KafkaConsumer<String, String> cons = new KafkaConsumer<String, String>(getConsumerProperties());
		cons.subscribe(Collections.singleton(this.consumerTopicName));
		return cons;
	}

    private final void connect() {    	
    	if (this.kafkaProducerProperties != null) {
	        try {
	        	this.producer = createProducer();
	        } catch (Exception e) {
	            tracer.error("Failed to create Kafka producer : ", e);
	        }
    	}
    	
    	if (this.kafkaConsumerProperties != null) {
	        try {
	        	this.consumer = createConsumer();
	        } catch (Exception e) {
	            tracer.error("Failed to create Kafka consumer : ", e);
	        }    		
    	}
    }

    private boolean isConnected() {
    	return (this.producer != null);
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
            	sendFile(producerTopicName, localTargetPath);
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
            	sendFile(producerTopicName, localTargetPath);
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
    		AdminClient adminClient = KafkaAdminClient.create(getProducerProperties());
    		List<NewTopic> newTopics = new ArrayList<NewTopic>(1);
    		short replFactor = getReplicationFactor();
    		newTopics.add(new NewTopic(producerTopicName, 1, (short) 1));
    		CreateTopicsResult res = adminClient.createTopics(newTopics);
    		res.all().get();
    		remoteWriteArchiveCreated = true;
        } catch(Exception e) {
        	if (e.getMessage().contains("Topic") && e.getMessage().contains("already exists")) {
        		remoteWriteArchiveCreated = true;
        		return;
        	} else {
        		throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
        	}
        }
    }

	private final short getReplicationFactor() {
		String replFactorStr = kafkaProducerProperties.get("replication-factor");
		if (replFactorStr == null) {
			return 1;
		} else {
			try {
				short r = Short.valueOf(replFactorStr);
				return r;
			} catch (IllegalArgumentException e) {
				tracer.error("Invalid value " +  replFactorStr + " specified for kafka replication-factor in props file");
				return 1;
			}
		}
	}

	private final void sendFile(String topic, Path filePath) throws IOException, InterruptedException, ExecutionException {
		String fileName = filePath.getFileName().toString();

		HashMap<String, byte[]> fileMap = new HashMap<String, byte[]>();
		ProducerRecord<String, Map> record;

		long fileSize = Files.size(filePath);
		if (fileSize > FILE_CHUNK_SIZE) {
			long numChunks = fileSize / FILE_CHUNK_SIZE;
			int lastChunkSize = (int) (fileSize - (numChunks * FILE_CHUNK_SIZE));
			try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
				int result = 0;
				long chunkIndex = 1;
				while (true){
					byte[] fileContent;
					if (chunkIndex  == numChunks) {
						fileContent = new byte[lastChunkSize];
					} else {
						fileContent = new byte[FILE_CHUNK_SIZE];
					}

					result = fis.read(fileContent);
					if (result == -1) {
						break;
					}
					fileMap.clear();
					String filePartName = fileName.toString() + ".part." + chunkIndex;
					fileMap.put(filePartName, fileContent);    	
					record = new ProducerRecord<String, Map>(topic, fileMap);
					this.producer.send(record).get();
					++chunkIndex;
				}
				
				//Send MERGE:<num_chunks> record
				fileMap.clear();
				String fileMergeMsg = "MERGE:" + numChunks;
				fileMap.put(fileName, fileMergeMsg.getBytes());
				record = new ProducerRecord<String, Map>(topic, fileMap);
				this.producer.send(record).get();				
			}
		} else {
			byte[] fileContent = Files.readAllBytes(filePath);    		
			fileMap.put(fileName, fileContent);    	
			record = new ProducerRecord<String, Map>(topic, fileMap);
			this.producer.send(record).get();
		}
	}
	
    @Override
    List<Path> getObjectsInReadArchive() throws SQLException {
    	try {
			ConsumerRecords<String, String> messages = consumer.poll(100);
	    	//List<String> commandFiles = new ArrayList<String>();
			for (ConsumerRecord<String, String> message : messages) {								
				String msg = message.value();
				//The part before first space character is COMMAND_NAME while the rest is COMMAND_DETAILS
		        String[] splitParts = msg.split(" ", 2);
		        
		        if (splitParts.length == 2) {
		        	Path dstFilePath = readArchivePath.resolve(splitParts[0]);
		        	Files.writeString(dstFilePath, splitParts[1], StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		        } else if (splitParts.length == 1) {
		        	Path dstFilePath = readArchivePath.resolve(splitParts[0]);
		        	Files.writeString(dstFilePath, "", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		        }
				
				//commandFiles.add(cmd);
			}
			return super.getObjectsInReadArchive();
    	} catch(Exception e) {
    		throw new RemoteShippingException("Failed to lookup readArchive : " + readArchivePath + " from remote location : " + consumerTopicName, e);
    	}
    }
    
	@Override
	void terminate() {		
		if (this.producer != null) {
			this.producer.close();
		}
	}
}
