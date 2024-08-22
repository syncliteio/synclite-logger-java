package io.synclite.logger;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public class CommandHandler {
	
    private final String readArchieveName;
    private final Path readArchievePath;
    private Logger tracer;
    private final MetadataManager metadataMgr;
    protected AtomicLong lastProcessedCommandTS = new AtomicLong(-1L);
    private SyncLiteOptions options;
    protected final FSArchiver reader;
    protected final ScheduledExecutorService cmdHandlerService;
    protected final Integer destIndex;

    CommandHandler(Path dbPath, String readArchieveName, MetadataManager metadataMgr, SyncLiteOptions options, Integer destIndex, Logger tracer) throws SQLException {
        this.readArchieveName = readArchieveName;
        this.readArchievePath = Path.of(options.getLocalCommandStageDirectory(destIndex).toString(), this.readArchieveName + "/");
        this.tracer = tracer;
        this.metadataMgr = metadataMgr;
        this.options = options;
        this.destIndex = destIndex;
        switch (options.getDestinationType(destIndex)) {
        case FS:
        case MS_ONEDRIVE:
        case GOOGLE_DRIVE:
            this.reader = new FSArchiver(dbPath, null, null, this.readArchieveName, this.readArchievePath, tracer, options.getEncryptionKeyFile());
            break;
        case SFTP:
            this.reader = new SFTPArchiver(dbPath, null, null, this.readArchieveName, this.readArchievePath, options.getHost(destIndex), options.getPort(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), options.getRemoteCommandStageDirectory(destIndex), tracer, options.getEncryptionKeyFile());
            break;
        case MINIO:
            this.reader = new MinioArchiver(dbPath, null, null, this.readArchieveName, this.readArchievePath, options.getHost(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), options.getRemoteCommandStageDirectory(destIndex), tracer, options.getEncryptionKeyFile());
            break;     
        case KAFKA:
			this.reader = new KafkaArchiver(dbPath, null, null, this.readArchieveName, this.readArchievePath, null, options.getKafkaConsumerProperties(destIndex), "command", tracer, options.getEncryptionKeyFile());
			break;
        case S3:
        	this.reader = new S3Archiver(dbPath, null, null, this.readArchieveName, this.readArchievePath, options.getHost(destIndex), options.getUserName(destIndex), options.getPassword(destIndex), options.getRemoteDataStageDirectory(destIndex), options.getRemoteCommandStageDirectory(destIndex), tracer, options.getEncryptionKeyFile());
        	break;
        default:
            throw new RuntimeException("Unsupported destination type : " + options.getLocalDataStageDirectory(destIndex));
        }
        
        Long longVal = this.metadataMgr.getLongProperty("last_processed_command_ts-" + destIndex);
        if (longVal != null) {
            this.lastProcessedCommandTS.set(longVal);
        } else {
            this.lastProcessedCommandTS.set(-1L);
            this.metadataMgr.insertProperty("last_processed_command_ts-" + destIndex, this.lastProcessedCommandTS);
        }

        longVal = this.metadataMgr.getLongProperty("last_processed_command_ts-" + destIndex);
        if (longVal != null) {
            this.lastProcessedCommandTS.set(longVal);
        } else {
            this.lastProcessedCommandTS.set(-1L);
            this.metadataMgr.insertProperty("last_processed_command_ts-" + destIndex, this.lastProcessedCommandTS);
        }

        cmdHandlerService = Executors.newScheduledThreadPool(1);
        cmdHandlerService.scheduleAtFixedRate(this::handleCommands, 0, options.getCommandHandlerFrequencyMs(), TimeUnit.MILLISECONDS);
    }
    
    public final void handleCommands() {
        try {
        	List<Path> commandFiles = this.reader.getObjectsInReadArchive();
        	if ((commandFiles == null) || (commandFiles.isEmpty())) {
        		return;
        	}
			TreeMap<Long, List<Object>> cmdsToExecute = new TreeMap<Long, List<Object>>();
			for (Path cmdFile : commandFiles) {
				String [] tokens = cmdFile.getFileName().toString().split("\\.");
				long cmdTS = 0;
				String cmdText = null;
				if (tokens.length == 2) {
					try {
						cmdTS = Long.parseLong(tokens[0]);
					} catch (NumberFormatException e) {
						//Skip this invalid commandfile and move on
						continue;
					}
					cmdText = tokens[1];
					
					ArrayList cmdList = new ArrayList<Object>();
					cmdList.add(cmdText);
					cmdList.add(cmdFile);
					cmdsToExecute.put(cmdTS, cmdList);
				}
			}
			
			//Execute cmds in order of cmdTS
			String commandHandler = options.getExternalCommandHandler();
			for (Map.Entry<Long, List<Object>> entry : cmdsToExecute.entrySet()) {
				long cmdTS = entry.getKey();
				if (cmdTS <= lastProcessedCommandTS.get()) {
					continue;
				}
				String cmdText = (String) entry.getValue().get(0);
				Path cmdFile = (Path) entry.getValue().get(1);
				try {
					if (options.getCommandHandlerType() == CommandHandlerType.EXTERNAL) {
						String cmdToInvoke = commandHandler.replace("<COMMAND>", cmdText);
						cmdToInvoke = cmdToInvoke.replace("<COMMAND_FILE>", "\"" + cmdFile.toString() + "\"");
						Process p = Runtime.getRuntime().exec(cmdToInvoke);
						p.waitFor();
						//tracer.debug("Command " + cmdToInvoke + " returned with exit code : " + p.exitValue());
					} else {
						options.getCommandHanderCallback().handleCommand(cmdText, cmdFile);
					}					
					//Delete command from local read archive					
					this.reader.deleteLocalObject(cmdFile);
				} catch (Exception e) {
					tracer.error("Failed to execute command : " + cmdTS + "." + cmdText, e);
				} finally {
	                metadataMgr.updateProperty("last_processed_command_ts-" + destIndex, cmdTS);
	                this.lastProcessedCommandTS.set(cmdTS);
				}
			}
        } catch (Exception e) {
        	tracer.error("SyncLite CommandHandler failed to handle commands with exception : ", e);
        	//We will keep retrying
        }
    }
    
    final void terminate() {
    	if ((cmdHandlerService != null) && (!cmdHandlerService.isTerminated())) {
    		cmdHandlerService.shutdown();
    		try {
    			cmdHandlerService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    		} catch (InterruptedException e) {
    			//Ignore
    		}
    		//Terminate archivers
    		
    		if (reader != null) {
    			reader.terminate();
    		}
    	}
    }

}
