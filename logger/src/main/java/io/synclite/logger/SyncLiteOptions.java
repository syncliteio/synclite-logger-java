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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SyncLiteOptions {
	//*****IMPORTANT*********
	//WHEN YOU ADD A NEW MEMBER VARIABLE HERE, MAKE SURE TO HANDLE IT IN copy METHOD
	//TODO: DO this in a better way , using object clone ?
	//************************************
	static final SyncLiteOptions defaultOptions = new SyncLiteOptions();
	private String deviceName = "";
	//private int logQueueSize = 10000000;
	private int logQueueSize = Integer.MAX_VALUE;
	private long logSegmentFlushBatchSize = 1000000;
	private long logSegmentSwitchLogCountThreshold = 1000000;
	private long logSegmentSwitchDurationThresholdMs = 5000;
	private long logSegmentShippingFrequencyMs = 5000;
	//private long logSegmentPageSize = 32768;
	private long logSegmentPageSize = 512;
	private long maxInlinedLogArgs = 16;
	private boolean usePreCreatedDataBackup = false;
	private boolean vacuumDataBackup = true;
	private boolean skipRestartRecovery = false;
	private boolean disableAsyncLoggingForTxnDevice = false;
	private boolean enableAsyncLoggingForAppenderDevice = false;
	private Path encryptionKeyFile = null;
	private List<String> includeTables = null;
	private List<String> excludeTables = null;
	private long databaseId = -1;
	private String uuid = null;
	private DeviceType deviceType;
	private Logger tracer;
	private HashMap<Integer, DestinationType> destTypes = new HashMap<Integer, DestinationType>();
	private HashMap<Integer, Path> localStageDirectories = new HashMap<Integer, Path>();
	private HashMap<Integer, Path> localCommandStageDirectories = new HashMap<Integer, Path>();
	private HashMap<Integer, String> hosts = new HashMap<Integer, String>();
	private HashMap<Integer, Integer> ports = new HashMap<Integer, Integer>();
	private HashMap<Integer, String> userNames = new HashMap<Integer, String>();
	private HashMap<Integer, String> passwords = new HashMap<Integer, String>();
	private HashMap<Integer, String> remoteDataStageDirectories = new HashMap<Integer, String>();
	private HashMap<Integer, String> remoteCommandStageDirectories = new HashMap<Integer, String>();
	private boolean enableCommandHandler = false;
	private CommandHandlerType commandHandlerType;
	private String externalCommandHandler = null;
	private long commandHandlerFrequencyMs = 10000;
	private SyncLiteCommandHandlerCallback commandHandlerCallback = null;
	private HashMap<Integer, HashMap<String, String>> kafkaProducerProperties = new HashMap<Integer, HashMap<String, String>>();
	private HashMap<Integer, HashMap<String, String>> kafkaConsumerProperties = new HashMap<Integer, HashMap<String, String>>();

	SyncLiteOptions copy() {
		SyncLiteOptions copy = new SyncLiteOptions();
		copy.disableAsyncLoggingForTxnDevice = this.disableAsyncLoggingForTxnDevice;
		copy.enableAsyncLoggingForAppenderDevice = this.enableAsyncLoggingForAppenderDevice;
		copy.encryptionKeyFile = this.encryptionKeyFile;
		copy.databaseId = this.databaseId;
		for (Map.Entry<Integer, DestinationType> entry : this.destTypes.entrySet()) {
			copy.destTypes.put(entry.getKey(), entry.getValue());
		}
		copy.deviceName = this.deviceName;
		if (this.excludeTables != null) {
			copy.excludeTables.addAll(this.excludeTables);
		}
		for (Map.Entry<Integer, String> entry : this.hosts.entrySet()) {
			copy.hosts.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<Integer, Integer> entry : this.ports.entrySet()) {
			copy.ports.put(entry.getKey(), entry.getValue());
		}
		if (this.includeTables != null) {
			copy.includeTables.addAll(this.includeTables);
		}
		for (Map.Entry<Integer, HashMap<String,String>> entry : this.kafkaProducerProperties.entrySet()) {
			HashMap<String, String> props = new HashMap<String, String>();
			
			for (Map.Entry<String, String> propEntry : entry.getValue().entrySet()) {
				props.put(propEntry.getKey(), propEntry.getValue());
			}
			copy.kafkaProducerProperties.put(entry.getKey(), props);
		}
		for (Map.Entry<Integer, HashMap<String,String>> entry : this.kafkaConsumerProperties.entrySet()) {
			HashMap<String, String> props = new HashMap<String, String>();
			
			for (Map.Entry<String, String> propEntry : entry.getValue().entrySet()) {
				props.put(propEntry.getKey(), propEntry.getValue());
			}
			copy.kafkaConsumerProperties.put(entry.getKey(), props);
		}
		for (Map.Entry<Integer, Path> entry : this.localStageDirectories.entrySet()) {
			copy.localStageDirectories.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<Integer, Path> entry : this.localCommandStageDirectories.entrySet()) {
			copy.localCommandStageDirectories.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<Integer, String> entry : this.remoteDataStageDirectories.entrySet()) {
			copy.remoteDataStageDirectories.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<Integer, String> entry : this.remoteCommandStageDirectories.entrySet()) {
			copy.remoteCommandStageDirectories.put(entry.getKey(), entry.getValue());
		}

		copy.logQueueSize = this.logQueueSize;
		copy.logSegmentFlushBatchSize = this.logSegmentFlushBatchSize;
		copy.logSegmentPageSize = this.logSegmentPageSize;
		copy.logSegmentShippingFrequencyMs = this.logSegmentShippingFrequencyMs;
		copy.logSegmentSwitchDurationThresholdMs = this.logSegmentSwitchDurationThresholdMs;
		copy.logSegmentSwitchLogCountThreshold = this.logSegmentSwitchLogCountThreshold;
		copy.maxInlinedLogArgs = this.maxInlinedLogArgs;
		for (Map.Entry<Integer, String> entry : this.passwords.entrySet()) {
			copy.passwords.put(entry.getKey(), entry.getValue());
		}
		copy.skipRestartRecovery = this.skipRestartRecovery;
		copy.tracer = this.tracer;
		copy.usePreCreatedDataBackup = this.usePreCreatedDataBackup;
		for (Map.Entry<Integer, String> entry : this.userNames.entrySet()) {
			copy.userNames.put(entry.getKey(), entry.getValue());
		}
		copy.uuid = this.uuid;
		copy.deviceType = this.deviceType;
		copy.vacuumDataBackup = this.vacuumDataBackup;
				
		copy.enableCommandHandler = this.enableCommandHandler;
		copy.commandHandlerType = this.commandHandlerType;
		copy.externalCommandHandler = this.externalCommandHandler;
		copy.commandHandlerFrequencyMs = this.commandHandlerFrequencyMs;
		copy.commandHandlerCallback = this.commandHandlerCallback;

		return copy;
	}

	public DestinationType getDestinationType(Integer index) {
		return destTypes.get(index);
	}

	public void setDestinationType(Integer index, DestinationType destType) {
		this.destTypes.put(index, destType);
	}

	public boolean isLocalDataStageDirectorySpecified() {
		return (this.localStageDirectories.size() > 0);
	}
	
	public Path getLocalDataStageDirectory(Integer index) {
		return localStageDirectories.get(index);
	}

	public void setLocalDataStageDirectory(Integer index, Path stageDir) {
		this.localStageDirectories.put(index, stageDir);
	}

	public Path getLocalCommandStageDirectory(Integer index) {
		return localCommandStageDirectories.get(index);
	}

	public void setLocalCommandStageDirectory(Integer index, Path stageDir) {
		this.localCommandStageDirectories.put(index, stageDir);
	}

	public String getRemoteDataStageDirectory(Integer index) {
		return remoteDataStageDirectories.get(index);
	}

	public void setRemoteDataStageDirectory(Integer index, String remoteStageDir) {
		this.remoteDataStageDirectories.put(index, remoteStageDir);
	}

	public String getRemoteCommandStageDirectory(Integer destIndex) {
		return remoteCommandStageDirectories.get(destIndex);
	}

	public void setRemoteCommandStageDirectory(Integer index, String remoteCommandDir) {
		this.remoteCommandStageDirectories.put(index, remoteCommandDir);
	}

	public String getDeviceName() {
		return deviceName;
	}

	public boolean getEnableCommandHandler() {
		return this.enableCommandHandler;
	}
	
	public void setEnableCommandHandler(boolean val) {
		this.enableCommandHandler = val;
	}
	
	public CommandHandlerType getCommandHandlerType() {
		return this.commandHandlerType;
	}
	
	public void setCommandHandlerType(CommandHandlerType val) {
		this.commandHandlerType = val;
	}
	
	public String getExternalCommandHandler() {
		return externalCommandHandler;
	}

	public void setCommandHandlerCallback(SyncLiteCommandHandlerCallback callback) {
		this.commandHandlerCallback = callback;
	}
	
	public SyncLiteCommandHandlerCallback getCommandHanderCallback() {
		return this.commandHandlerCallback;
	}
	
	public void setCommandHandlerFrequencyMs(long frequency) throws SQLException {
		if (frequency<= 0) {
			throw new SQLException("SyncLite : Invalid value " + frequency + " specified for command handler frequency");
		}
		commandHandlerFrequencyMs = frequency;
	}

	public long getCommandHandlerFrequencyMs() {
		return commandHandlerFrequencyMs;
	}

	public void setDeviceName(String deviceName) throws SQLException {
		if (deviceName.matches("^[a-zA-Z0-9]*$")) {
			if (deviceName.length() > 64) {
				throw new SQLException("SyncLite : device name must be upto 64 alphanumeric characters");
			} else { 
				this.deviceName = deviceName;
			}
		} else {
			throw new SQLException("SyncLite : device name must contain alphanumeric characters only");
		}
		/*if (deviceName.contains(",")) {
            throw new SQLException("SyncLite : Character , not allowed in device name");
        }
        if (deviceName.contains("_")) {
            throw new SQLException("SyncLite : Character _ not allowed in device name");
        }
        this.deviceName = deviceName;
		 */
	}
	
	
	public void setExternalCommandHandler(String handler) {
		this.externalCommandHandler = handler;
	}

	long getDatabaseId() throws SQLException {
		return this.databaseId;
	}

	void setDatabaseId(long deviceDBID) throws SQLException {
		this.databaseId = deviceDBID;
	}

	void setUUID(String deviceUUID) throws SQLException {
		this.uuid = deviceUUID;
	}

	String getUUID() throws SQLException {
		return this.uuid;
	}
	
	void SetDeviceType(DeviceType type) throws SQLException {
		this.deviceType = type;
	}
	
	DeviceType getDeviceType() {
		return this.deviceType;
	}

	Logger getTracer() {
		return this.tracer;
	}

	void setTracer(Logger tracer) {
		this.tracer = tracer;
	}

	public String getHost(Integer index) {
		return hosts.get(index);
	}

	public void setHost(Integer index, String hostName) {
		this.hosts.put(index, hostName);
	}

	public Integer getPort(Integer index) {
		return ports.get(index);
	}

	public Integer setPort(Integer index, Integer port) {
		return ports.put(index, port);
	}

	public HashMap<String, String> getKafkaProducerProperties(Integer destIndex) {
		return kafkaProducerProperties.get(destIndex);
	}

	public HashMap<String, String> getKafkaConsumerProperties(Integer destIndex) {
		return kafkaConsumerProperties.get(destIndex);
	}

	public void setKafkaProducerProperty(Integer index, String propName, String propValue) {
		HashMap<String, String> kafkaProps = kafkaProducerProperties.get(index);
		if (kafkaProps == null) {
			kafkaProps = new HashMap<String, String>();
			kafkaProducerProperties.put(index, kafkaProps);
		}
		kafkaProps.put(propName, propValue);
	}

	public void setKafkaConsumerProperty(Integer index, String propName, String propValue) {
		HashMap<String, String> kafkaProps = kafkaConsumerProperties.get(index);
		if (kafkaProps == null) {
			kafkaProps = new HashMap<String, String>();
			kafkaConsumerProperties.put(index, kafkaProps);
		}
		kafkaProps.put(propName, propValue);
	}

	public String getKafkaProducerPropertyValue(Integer index, String propName) {
		HashMap<String, String> kafkaProps = kafkaProducerProperties.get(index);
		if (kafkaProps == null) {
			return null;
		}
		return kafkaProps.get(propName);
	}

	public String getKafkaConsumerPropertyValue(Integer index, String propName) {
		HashMap<String, String> kafkaProps = kafkaConsumerProperties.get(index);
		if (kafkaProps == null) {
			return null;
		}
		return kafkaProps.get(propName);
	}

	public String getUserName(Integer index) {
		return userNames.get(index);
	}

	public void setUserName(Integer index, String userName) {
		this.userNames.put(index, userName);
	}

	public String getPassword(Integer index) {
		return this.passwords.get(index);
	}

	public void setPassword(Integer index, String password) {
		this.passwords.put(index, password);
	}

	public int getLogQueueSize() {
		return logQueueSize;
	}

	public void setLogQueueSize(int size) throws SQLException {
		if (logQueueSize <= 0) {
			throw new SQLException("SyncLite : Invalid value " + size + " specified for log queue size");
		}
		logQueueSize = size;
	}

	public boolean getDisableAsyncLoggingForTxnDevice() {
		return this.disableAsyncLoggingForTxnDevice;
	}

	public boolean getEnableAsyncLoggingForAppenderDevice() {
		return this.enableAsyncLoggingForAppenderDevice;
	}

	public void includeTables(List<String> includeSet) throws SQLException {
		if (excludeTables != null) {
			throw new SQLException("SyncLite : Cannot specify include tables and exclude tables at the same time");
		}
		includeTables = includeSet;
	}

	public List<String> getIncludeTables() {
		return includeTables;
	}

	public void excludeTables(List<String> excludeSet) throws SQLException {
		if (includeTables != null) {
			throw new SQLException("SyncLite : Cannot specify include tables and exclude tables at the same time");
		}
		excludeTables = excludeSet;
	}

	public List<String> getExcludeTables() {
		return excludeTables;
	}

	public void setUsePreCreatedDataBackup(boolean useBackup) {
		usePreCreatedDataBackup = useBackup;
	}

	public boolean usePreCreatedDataBackup() {
		return usePreCreatedDataBackup;
	}

	public void setVacuumDataBackup(boolean vacuum) {
		vacuumDataBackup = vacuum;
	}
	public boolean getVacuumDataBackup() {
		return vacuumDataBackup;
	}

	public void setSkipRestartRecovery(boolean skip) {
		skipRestartRecovery = skip;
	}

	public void disableAsyncLoggingForTxnDevice(boolean async) {
		disableAsyncLoggingForTxnDevice = async;
	}

	public void enableAsyncLoggingForAppenderDevice(boolean async) {
		disableAsyncLoggingForTxnDevice = async;
	}

	public void setEncryptionKeyFile(Path pubKeyPath) {
		this.encryptionKeyFile = pubKeyPath;
	}
	
	public Path getEncryptionKeyFile() {
		return this.encryptionKeyFile;		
	}
	
	public boolean getSkipRestartRecovery() {
		return skipRestartRecovery;
	}


	public void setLogMaxInlineArgs(long argCnt) throws SQLException {
		if (argCnt <= 0) {
			throw new SQLException("SyncLite : Invalid value " + argCnt + " specified for log inlined argument count");
		}
		maxInlinedLogArgs = argCnt;
	}

	public long getLogMaxInlinedArgs() {
		return maxInlinedLogArgs;
	}

	public void setLogSegmentFlushBatchSize(long batchSize) throws SQLException {
		if (batchSize<= 0) {
			throw new SQLException("SyncLite : Invalid value " + batchSize+ " specified for log segment flush batch size");
		}
		logSegmentFlushBatchSize = batchSize;
	}

	public long getLogSegmentFlushBatchSize() {
		return logSegmentFlushBatchSize;
	}

	public void setLogSegmentShippingFrequencyMs(long frequency) throws SQLException {
		if (frequency<= 0) {
			throw new SQLException("SyncLite : Invalid value " + frequency + " specified for log segment shipping frequency");
		}
		logSegmentShippingFrequencyMs = frequency;
	}

	public long getLogSegmentShippingFrequencyMs() {
		return logSegmentShippingFrequencyMs;
	}

	public void setLogSegmentSwitchLogCountThreshold(long logCountThreshold) throws SQLException {
		if (logCountThreshold <= 0) {
			throw new SQLException("SyncLite : Invalid value " + logCountThreshold+ " specified for log segment switch log count threshold");
		}
		logSegmentSwitchLogCountThreshold = logCountThreshold;
	}

	public long getLogSegmentSwitchLogCountThreshold() {
		return logSegmentSwitchLogCountThreshold;
	}

	public void setLogSegmentSwitchDurationThresholdMs(long logDurationThreshold) throws SQLException {
		if (logDurationThreshold <= 0) {
			throw new SQLException("SyncLite : Invalid value " + logDurationThreshold+ " specified for log segment switch duration threshold");
		}
		logSegmentSwitchDurationThresholdMs = logDurationThreshold;
	}

	public long getLogSegmentSwitchDurationThresholdMs() {
		return logSegmentSwitchDurationThresholdMs;
	}

	public void setLogSegmentPageSize(long pageSize) throws SQLException {
		if ((pageSize < 512) || (pageSize > 65536)) {
			throw new SQLException("SyncLite : Invalid log segment page size specified. Value must be between [512, 65536].");
		}
		logSegmentPageSize = pageSize;
	}

	public long getLogSegmentPageSize() {
		return logSegmentPageSize;
	}

	public static SyncLiteOptions loadFromFile(Path propsPath) throws SQLException{
		return loadAndValidateOptions(propsPath, null);
	}

	public static SyncLiteOptions loadFromProps(Properties props) throws SQLException{
		HashMap<String, String> properties = new HashMap<String, String>();
	    for (String key : props.stringPropertyNames()) {
	    	properties.put(key, props.getProperty(key));
        }	
		SyncLiteOptions options = new SyncLiteOptions();
		options.setTracer(null);
		validateAndSetOptions(options, properties, null);
		return options;
	}

	static SyncLiteOptions loadAndValidateOptions(Path propsPath, Logger tracer) throws SQLException {
		BufferedReader reader = null;
		HashMap<String, String> properties = new HashMap<String, String>();
		SyncLiteOptions options = new SyncLiteOptions();
		options.setTracer(tracer);
		try {
			reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=", 2);
				if (tokens.length < 2) {
					throw new SQLException("Invalid line in property file " + propsPath + " : " + line);
				}
				String optName = tokens[0].trim().toLowerCase();
				String optVal = line.substring(line.indexOf("=") + 1, line.length()).trim();
				properties.put(optName, optVal);
				line = reader.readLine();
			}
		} catch (IOException e) {
			throw new SQLException("Failed to load property file : " + propsPath + " : ", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new SQLException("Failed to close property file : " + propsPath + ": " , e);
				}
			}
		}
		validateAndSetOptions(options, properties, tracer);
		return options;
	}

	private static void validateAndSetOptions(SyncLiteOptions options, HashMap<String, String> properties, Logger tracer) throws SQLException {
		try {
			String optVal = properties.get("device-name");
			if (optVal != null) {
				options.setDeviceName(optVal);
			}

			optVal = properties.get("log-queue-size");
			if (optVal != null) {
				Integer val = Integer.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-queue-size in configuration file");
				} else {
					options.setLogQueueSize(val);
				}
			}

			optVal = properties.get("log-segment-flush-batch-size");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-segment-flush-batch-size in configuration file");
				} else {
					options.setLogSegmentFlushBatchSize(val);
				}
			}

			optVal = properties.get("log-segment-switch-log-count-threshold");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-segment-switch-log-count-threshold in configuration file");
				} else {
					options.setLogSegmentSwitchLogCountThreshold(val);
				}
			}

			optVal = properties.get("log-segment-switch-duration-threshold-ms");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-segment-switch-duration-threshold-ms in configuration file");
				} else {
					options.setLogSegmentSwitchDurationThresholdMs(val);
				}
			}

			optVal = properties.get("log-segment-shipping-frequency-ms");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-segment-shipping-frequency-ms in configuration file");
				} else {
					options.setLogSegmentShippingFrequencyMs(val);
				}
			}

			optVal = properties.get("log-segment-page-size");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-segment-page-size in configuration file");
				} else {
					options.setLogSegmentPageSize(val);
				}
			}

			optVal = properties.get("log-max-inlined-arg-count");
			if (optVal != null) {
				Long val = Long.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for log-max-inlined-arg-count in configuration file");
				} else {
					options.setLogMaxInlineArgs(val);
				}
			}

			optVal = properties.get("use-precreated-data-backup");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for use-precreated-data-backup in configuration file");
				} else {
					options.setUsePreCreatedDataBackup(val);
				}
			}

			optVal = properties.get("vacuum-data-backup");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for vacuum-data-backup in configuration file");
				} else {
					options.setVacuumDataBackup(val);
				}
			}

			optVal = properties.get("skip-restart-recovery");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for skip-restart-recovery in configuration file");
				} else {
					options.setSkipRestartRecovery(val);
				}   
			}

			optVal = properties.get("disable-async-logging-for-transactional-device");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for disable-async-logging-for-transactional-device in configuration file");
				} else {
					options.disableAsyncLoggingForTxnDevice(val);
				}   
			} 

			optVal = properties.get("enable-async-logging-for-appender-device");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for enable-async-logging-for-appender-device in configuration file");
				} else {
					options.enableAsyncLoggingForAppenderDevice(val);
				}   
			}

			optVal = properties.get("device-encryption-key-file");
			if (optVal != null) {
				Path val = Path.of(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for device-encryption-key-file in configuration file");
				} else {
					if (Files.exists(val)) {
						options.setEncryptionKeyFile(val);
					} else {
						throw new SQLException("SyncLite : Invalid file " + optVal + " specified for encryption key file");
					}
				}   
			}

			optVal = properties.get("include-tables");
			if (optVal != null) {
				String[] tokens = optVal.split(",");
				if (tokens.length > 0) {
					options.includeTables(Arrays.asList(tokens));
				} else {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for include-tables in configuration file. Please specify a comma separated table list.");
				}
			}

			optVal = properties.get("exclude-tables");
			if (optVal != null) {
				String[] tokens = optVal.split(",");
				if (tokens.length > 0) {
					options.excludeTables(Arrays.asList(tokens));
				} else {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for exclude-tables in configuration file. Please specify a comma separated table list.");
				}
			}

			optVal = properties.get("enable-command-handler");
			if (optVal != null) {
				Boolean val = Boolean.valueOf(optVal);
				if (val == null) {
					throw new SQLException("SyncLite : Invalid value " + optVal + " specified for enable-command-handler in configuration file");
				} else {
					options.setEnableCommandHandler(val);

					optVal = properties.get("command-handler-type");
					CommandHandlerType cmdHandlerType = CommandHandlerType.EXTERNAL;
					if (optVal != null) {
						try {
							cmdHandlerType = CommandHandlerType.valueOf(optVal);
						} catch (Exception e) {
							throw new SQLException("SyncLite : Invalid value " + optVal + " specified for command-handler-type in configuration file"); 
						}
						options.setCommandHandlerType(cmdHandlerType);
					} else {
						options.setCommandHandlerType(CommandHandlerType.EXTERNAL);
					}

					if (cmdHandlerType == CommandHandlerType.EXTERNAL) {	
						optVal = properties.get("external-command-handler");
						if (optVal != null) {
							if ((!optVal.contains("<COMMAND>")) || (!optVal.contains("<COMMAND_FILE>"))) {
								throw new SQLException("SyncLite : Invalid value " + optVal + " specified for external-command-handler in configuration file, command-handler must include placeholders <COMMAND> and <COMMAND_FILE>");
							}
							options.setExternalCommandHandler(optVal);
						} else {
							throw new SQLException("SyncLite : No external-command-handler specified in configuration file");							
						}
					}

					optVal = properties.get("command-handler-frequency-ms");
					if (optVal != null) {
						Long frequency = Long.valueOf(optVal);
						if (frequency == null) {
							throw new SQLException("SyncLite : Invalid value " + optVal + " specified for command-handler-frequency-ms in configuration file");
						} else {
							options.setCommandHandlerFrequencyMs(frequency);
						}
					} else {
						options.setCommandHandlerFrequencyMs(10000);
					}
				}   
			}

			Integer destIndex=1;
			if (properties.containsKey("destination-type")) {
				parseDestination(properties, destIndex, "", options);
			} else if (properties.containsKey("destination-type-1")) {
				while(true) {
					if (properties.containsKey("destination-type-" + destIndex)) {
						parseDestination(properties, destIndex, "-" + destIndex, options);
					} else {
						break;
					}
					++destIndex;
				}
			} else {
				//If no destinations are specified then assume it to be FS with local-data-stage-directory same as that of device base
				properties.put("destination-type", DestinationType.FS.toString());
				parseDestination(properties, destIndex, "", options);
			}
		} catch (SQLException e) {
			if (tracer != null) {
				tracer.error("Failed to load options : "  + e.getMessage(), e);
			}
			throw e;
		}
	}

	private static void parseDestination(HashMap<String, String> properties, Integer destIndex, String propSuffix, SyncLiteOptions options) throws SQLException {
		if (properties.containsKey("destination-type" + propSuffix)) {
			//Single destination is supplied. Parse all values
			String optVal = properties.get("destination-type" + propSuffix);
			DestinationType destType;
			try {
				destType = DestinationType.valueOf(optVal);
			} catch (IllegalArgumentException e) {
				throw new SQLException("SyncLite : Invalid value " + optVal + " specified for destination-type" + propSuffix);
			}
			if (destType == null) {
				throw new SQLException("SyncLite : Invalid value " + optVal + " specified for destination-type" + propSuffix);
			} else {
				options.setDestinationType(1, destType);
			}

			//Parse other properties    			
			if ((destType == DestinationType.FS) || (destType == DestinationType.MS_ONEDRIVE) || (destType == DestinationType.GOOGLE_DRIVE)) {
				//local-data-stage-directory must be specified
				optVal = properties.get("local-data-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isWritable(val)) {
							options.setLocalDataStageDirectory(destIndex, val);
						} else {							
							throw new SQLException("SyncLite : Specified local-data-stage-directory" + propSuffix + " : " + optVal + " does not have write permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " specified for local-data-stage-directory" + propSuffix);
					}
				} else {
					//Do not throw this for FS if multiple destinations are not specified. We will default to the same path as that of the db file. 
					if ((destType != DestinationType.FS) || (!propSuffix.isBlank())) {
						throw new SQLException("SyncLite : No local-data-stage-directory" + propSuffix);
					}						
				}

				optVal = properties.get("local-command-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isReadable(val)) {
							options.setLocalCommandStageDirectory(destIndex, val);
						} else {
							throw new SQLException("SyncLite : Specified local-command-stage-directory" + propSuffix + " : " + optVal + " does not have read permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " specified for local-command-stage-directory" + propSuffix);
					}
				} else {
					if (options.getEnableCommandHandler()) {
						throw new SQLException("SyncLite : No local-command-stage-directory" + propSuffix + ", it must be specified while command handler is enabled");
					}
				}
			} else if (destType == DestinationType.SFTP) {
				//local-data-stage-directory must be specified    				
				optVal = properties.get("local-data-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isWritable(val)) {
							options.setLocalDataStageDirectory(destIndex, val);
						} else {
							throw new SQLException("SyncLite : Specified local-data-stage-directory" + propSuffix + " : " + optVal + " does not have write permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " specified for local-data-stage-directory" + propSuffix);
					}
				} else {
					throw new SQLException("SyncLite : local-data-stage-directory" + propSuffix);
				}
				//Parse sftp properties
				optVal = properties.get("sftp" + propSuffix + ":host");
				if (optVal != null) {
					options.setHost(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : SFTP host" + propSuffix +" not specified");
				}

				//Parse sftp properties
				optVal = properties.get("sftp" + propSuffix + ":port");
				if (optVal != null) {
					try {
						options.setPort(destIndex, Integer.valueOf(optVal));
					} catch (NumberFormatException e) {
						throw new SQLException("SyncLite : Invalid value specified for sftp" + propSuffix +":port");
					}
				} else {
					throw new SQLException("SyncLite : sftp" + propSuffix +":port");
				}

				optVal = properties.get("sftp" + propSuffix + ":user-name");
				if (optVal != null) {
					options.setUserName(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : sftp" + propSuffix + ":user-name not specified");
				}

				optVal = properties.get("sftp" + propSuffix + ":password");
				if (optVal != null) {
					options.setPassword(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : sftp" + propSuffix + ":password not specified");
				}

				optVal = properties.get("sftp" + propSuffix + ":remote-data-stage-directory");
				if (optVal != null) {
					options.setRemoteDataStageDirectory(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : sftp" + propSuffix + ":remote-data-stage-directory not specified");
				}

				if (options.getEnableCommandHandler()) {
					optVal = properties.get("remote-command-stage-directory" + propSuffix);
					if (optVal != null) {
						options.setRemoteCommandStageDirectory(destIndex, optVal);
					} else {
						throw new SQLException("SyncLite : sftp" + propSuffix + ":remote-command-stage-directory not specified, while device command handler is enabled");
					}
				}
			} else if (destType == DestinationType.MINIO) {
				//local-data-stage-directory must be specified    				
				optVal = properties.get("local-data-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isWritable(val)) {
							options.setLocalDataStageDirectory(destIndex, val);
						} else {
							throw new SQLException("SyncLite : Specified local-data-stage-directory" + propSuffix + " : " + optVal + " does not have write permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " not specified for local-data-stage-directory" + propSuffix);
					}
				} else {
					throw new SQLException("SyncLite : local-data-stage-directory" + propSuffix + " not specified");
				}
				//Parse MinIO properties
				optVal = properties.get("minio" + propSuffix + ":endpoint");
				if (optVal != null) {
					options.setHost(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : minio" + propSuffix +":endpoint not specified");
				}

				optVal = properties.get("minio" + propSuffix + ":access-key");
				if (optVal != null) {
					options.setUserName(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : minio" + propSuffix + ":access-key not specified");
				}

				optVal = properties.get("minio" + propSuffix + ":secret-key");
				if (optVal != null) {
					options.setPassword(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : minio" + propSuffix + ":secret-key not specified");
				}

				optVal = properties.get("minio" + propSuffix + ":data-stage-bucket-name");
				if (optVal != null) {
					options.setRemoteDataStageDirectory(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : minio" + propSuffix + ":data-stage-bucket-name not specified");
				}

				if (options.getEnableCommandHandler()) {
					optVal = properties.get("minio" + propSuffix + ":command-stage-bucket-name");
					if (optVal != null) {
						options.setRemoteCommandStageDirectory(destIndex, optVal);
					} else {
						throw new SQLException("SyncLite : minio" + propSuffix + ":command-stage-bucket-name not specified, it must be specified when device command handler is enabled");
					}
				}

			} else if (destType == DestinationType.S3) {
				//local-data-stage-directory must be specified    				
				optVal = properties.get("local-data-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isWritable(val)) {
							options.setLocalDataStageDirectory(destIndex, val);
						} else {
							throw new SQLException("SyncLite : Specified local-data-stage-directory" + propSuffix + " : " + optVal + " does not have write permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " not specified for local-data-stage-directory" + propSuffix);
					}
				} else {
					throw new SQLException("SyncLite : local-data-stage-directory" + propSuffix + " not specified");
				}
				//Parse S3 properties
				optVal = properties.get("s3" + propSuffix + ":endpoint");
				if (optVal != null) {
					options.setHost(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : s3" + propSuffix +":endpoint not specified");
				}

				optVal = properties.get("s3" + propSuffix + ":data-stage-bucket-name");
				if (optVal != null) {
					options.setRemoteDataStageDirectory(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : s3" + propSuffix +":data-stage-bucket-name not specified");
				}

				optVal = properties.get("s3" + propSuffix + ":access-key");
				if (optVal != null) {
					options.setUserName(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : s3" + propSuffix + ":access-key not specified");
				}

				optVal = properties.get("s3" + propSuffix + ":secret-key");
				if (optVal != null) {
					options.setPassword(destIndex, optVal);
				} else {
					throw new SQLException("SyncLite : s3" + propSuffix + ":secret-key not specified");
				}
				
				optVal = properties.get("s3" + propSuffix + ":command-stage-bucket-name");
				if (options.getEnableCommandHandler()) {
					if (optVal != null) {
						options.setRemoteCommandStageDirectory(destIndex, optVal);
					} else {
						throw new SQLException("SyncLite : s3" + propSuffix + ":command-stage-bucket-name not specified, while device command handler is enabled");
					}
				}
			} else if (destType == DestinationType.KAFKA) {
				//local-data-stage-directory must be specified    				
				optVal = properties.get("local-data-stage-directory" + propSuffix);
				if (optVal != null) {
					Path val = Path.of(optVal);
					if (!Files.exists(val)) {
						try {
							//Try creating the specified local path
							Files.createDirectories(val);
						} catch (IOException e) {
							//Ignore here
						}
					}
					if (Files.exists(val)) {
						if (Files.isWritable(val)) {
							options.setLocalDataStageDirectory(destIndex, val);
						} else {
							throw new SQLException("SyncLite : Specified local-data-stage-directory" + propSuffix + " : " + optVal + " does not have write permission");
						}
					} else {
						throw new SQLException("SyncLite : Invalid value " + optVal + " specified for local-data-stage-directory" + propSuffix);
					}
				} else {
					throw new SQLException("SyncLite : local-data-stage-directory" + propSuffix + " not specified");
				}
				
				//Look for Kafka producer properties
				for (Map.Entry<String, String> entry : properties.entrySet()) {
					String propName = entry.getKey();
					String propValue = entry.getValue();
					if (propName.startsWith("kafka-producer" + propSuffix)) {
						String kafkaPropName = propName.substring(propName.indexOf(":")+1);
						if (kafkaPropName == null) {
							throw new SQLException("SyncLite : Invalid kafka producer property specified  : " + propName);
						} else {
							options.setKafkaProducerProperty(destIndex, kafkaPropName, propValue);
						}						
					}
				}

				if (options.getKafkaProducerPropertyValue(destIndex, "bootstrap.servers") == null) {
					throw new SQLException("SyncLite : kafka-producer:bootstrap.servers property not specified for Kafka destination " + destIndex);
				}

				
				//Look for Kafka consumer properties
				boolean foundConsumerProps = false;
				for (Map.Entry<String, String> entry : properties.entrySet()) {
					String propName = entry.getKey();
					String propValue = entry.getValue();
					if (propName.startsWith("kafka-consumer" + propSuffix)) {
						String kafkaPropName = propName.substring(propName.indexOf(":")+1);
						if (kafkaPropName == null) {
							throw new SQLException("SyncLite : Invalid kafka consumer property specified  : " + propName);
						} else {
							options.setKafkaConsumerProperty(destIndex, kafkaPropName, propValue);
						}						
					}
					foundConsumerProps = true;
				}
				
				if (foundConsumerProps) {
					if (options.getKafkaConsumerPropertyValue(destIndex, "bootstrap.servers") == null) {
						throw new SQLException("SyncLite : kafka-consumer" + propSuffix + ":bootstrap.servers property not specified for Kafka destination " + destIndex);
					}					
				} else {
					if (options.getEnableCommandHandler()) {
						throw new SQLException("SyncLite : kafka-consumer" + propSuffix + " properties not specified, it must be specified when device command handler is enabled, while device command handler is enabled ");
					}
				}
			} else {
				if (propSuffix.isEmpty()) {
					//If no destinations are specified at all  
				}
				throw new SQLException("SyncLite : No destinations specified");
			}
		}
	}

	Integer getNumDestinations() {
		return destTypes.size();
	}

}
