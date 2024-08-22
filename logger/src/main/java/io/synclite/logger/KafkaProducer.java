package io.synclite.logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducer extends org.apache.kafka.clients.producer.KafkaProducer<String,String> {
	SyncLiteOptions options;
	Properties props;
	int maxBatchSizeBytes;
	//private final Serializer<String> keySerializer;
    //private final Serializer<String> valueSerializer;
    private Path dbPath;
    private DeviceType deviceType;
    
	public KafkaProducer(Path configFile) throws Exception{
		this(loadProps(configFile));
	}
	

	private static Properties loadProps(Path configFile) throws Exception {
		Properties props = new Properties();
		try (FileInputStream input = new FileInputStream(configFile.toString())) {
			props.load(input);
		} catch (IOException ex) {
			throw new Exception(ex);
		}
		return props;
	}

	public KafkaProducer(Properties properties) throws Exception {
		super(withDefaults(properties));
		props = properties;
		options = SyncLiteOptions.loadFromProps(properties);
		setDefaults();
		//this.keySerializer = (Serializer<String>) getConfiguredInstance(properties, org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        //this.valueSerializer = (Serializer<String>) getConfiguredInstance(properties, org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);        
	}

	private static Properties withDefaults(Properties props) {
		int batchSize = 1024 * 1024;
		if ( !props.containsKey(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG)) {
			props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		}

		int lingerMS = 5000;
		if (! props.containsKey(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG)) {
			props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, lingerMS);
		}

		String bootstrapURLs = "localhost:9092";
		if (! props.containsKey(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapURLs);
		}
		
		if (! props.containsKey(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		}
		
		if (! props.containsKey(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		}
		
		return props;
	}


	private void setDefaults() throws Exception {
    	 this.maxBatchSizeBytes = (int) props.get(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG); 

		 int lingerMS = (int) props.get(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG);

    	 this.options.setLogSegmentSwitchDurationThresholdMs(lingerMS);

    	 this.dbPath = Path.of(System.getProperty("user.home"), "synclite", "job1", "db");
    	 if (this.props.containsKey("device-path")) {
    		 this.dbPath = Path.of(this.props.get("device-path").toString());
    	 } 

    	 if (!Files.exists(this.dbPath)) {
	         try {
	          	Files.createDirectories(this.dbPath);
	          } catch(IOException e) {
	          	throw new KafkaException("Failed to initialize specified db-path : " + this.dbPath + " : " + e.getMessage(), e);
	          }
		 }
		 if (!Files.exists(this.dbPath)) {
			 throw new ConfigException("Specified db-path : " + this.dbPath + " does not exist");
		 }

    	 this.deviceType = DeviceType.STREAMING;
    	 if (this.props.containsKey("device-type")) {
    		 try {
    			 this.deviceType = DeviceType.valueOf(this.props.get("device-type").toString());
    			 if ((this.deviceType != DeviceType.TELEMETRY) && (this.deviceType != DeviceType.STREAMING) && (this.deviceType != DeviceType.SQLITE_APPENDER)) {
    				 throw new ConfigException("Specified device-type : " + this.deviceType + " is not supported. Supported SyncLite device types are STREAMING, TELEMETRY and APPENDER");
    			 }
    		 } catch(Exception e) {
    			 throw new ConfigException("Specified device-type : " + this.deviceType + " is invalid");
    		 }    		 
    	 } 
		 
         if (!options.isLocalDataStageDirectorySpecified()) {
             Path stageDirPath = Path.of(System.getProperty("user.home"), "synclite", "job1", "stageDir");
             try {
             	Files.createDirectories(stageDirPath);
             } catch(IOException e) {
             	throw new KafkaException("Failed to initialize specified local-data-stage-directory : " + stageDirPath + " : " + e.getMessage(), e);
             }
        	 this.options.setLocalDataStageDirectory(1, stageDirPath);
         }
    }

	private static <T> T getConfiguredInstance(Properties properties, String configKey, Class<T> defaultClass) {
        String className = properties.getProperty(configKey);
        if (className == null) {
            try {
                return defaultClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate default class for " + configKey, e);
            }
        }
        try {
            Class<?> clazz = Class.forName(className);
            return (T) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class for " + configKey, e);
        }
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
    	return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        return doSend(record, callback);
    }

    @Override
	public void flush() {
    	try {
    		DeviceWriter.flush();
    	} catch(SQLException e) {
    		throw new KafkaException("Failed to flush KafkaProducer : " + e.getMessage(), e);
    	}
    }

    private Future<RecordMetadata> doSend(ProducerRecord<String, String> record, Callback callback) {
		CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
		try {
			// Simulate message sending logic
			// Serialize the key and value
			//byte[] serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
			//byte[] serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());

			// Print the key and value
			//System.out.println("Sending message with key: " + record.key() + " and value: " + record.value());
						
			long offset = writeMessage(record.topic(), record.key(), record.value());
			
			TopicPartition partition = new TopicPartition(record.topic(), 0);
			long timestamp = System.currentTimeMillis();
			//long serializedKeySize = serializedKey != null ? serializedKey.length : -1;
			//long serializedValueSize = serializedValue != null ? serializedValue.length : -1;

			RecordMetadata metadata = new RecordMetadata(
					partition, 
					offset, 
					0,
					timestamp, 
					record.key().length(),
					record.value().length()
					);

			// Complete the future
			future.complete(metadata);

			// Execute the callback if provided
			if (callback != null) {
				callback.onCompletion(metadata, null);
			}

		} catch (Exception e) {
			// Handle exceptions and complete the future exceptionally
			future.completeExceptionally(e);

			// Execute the callback with the exception
			if (callback != null) {
				callback.onCompletion(null, e);
			}
		}

		return future;
	}
    
    @Override
    public void initTransactions() {
    	throw new IllegalStateException("Unsupported operation initTransactions in SyncLite KafkaProducer");
    }

    @Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
            ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
    	throw new IllegalStateException("Unsupported operation sendOffsetToTransaction in SyncLite KafkaProducer");
    }
    
    @Override
    public void beginTransaction() throws ProducerFencedException {
    	throw new IllegalStateException("Unsupported operation beginTransaction in SyncLite KafkaProducer");    	
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
    	throw new IllegalStateException("Unsupported operation commitTransaction in SyncLite KafkaProducer");    	
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
    	throw new IllegalStateException("Unsupported operation rollbackTransaction in SyncLite KafkaProducer");    	
    }

    final private long writeMessage(String topic, String key, String value) throws Exception{
    	//get or create device    	
    	DeviceWriter wrt = DeviceWriter.getInstance(this.dbPath, this.deviceType, this.options, this.maxBatchSizeBytes);    	
    	//write record    	
    	//One writer can be used by one thread at any point.
    	synchronized(wrt) {
	    	long offset = wrt.write(topic, key, value);
	    	return offset;
    	}
    }
    
    @Override
    public void close() {
    	try {
    		DeviceWriter.close();
    	} catch (SQLException e) {
    		throw new KafkaException("Failed to close KafkaProducer : " + e.getMessage(), e);
    	}
    }

}
