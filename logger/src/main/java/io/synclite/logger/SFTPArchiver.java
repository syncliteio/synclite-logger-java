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

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

class SFTPArchiver extends FSArchiver {

    private boolean remoteWriteArchiveCreated;
    private Session session;
    private ChannelSftp channel;
    private String host;
    private Integer port;
    private String user;
    private String password;
    private String remoteDataDirectory;
    private String remoteCommandDirectory;

    SFTPArchiver(Path dbPath, String writeArchiveName, Path writeArchivePath, String readArchiveName, Path readArchivePath, String host, Integer port, String user, String password, String remoteDataDirectory, String remoteCommandDirectory, Logger tracer, Path encryptionKeyPath) {
        super(dbPath, writeArchiveName, writeArchivePath, readArchiveName, readArchivePath, tracer, encryptionKeyPath);
        this.remoteWriteArchiveCreated = false;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.remoteDataDirectory = remoteDataDirectory.replace("\\", "/");
        if (this.remoteCommandDirectory != null) {
        	this.remoteCommandDirectory = remoteCommandDirectory.replace("\\", "/");
        }
        //connect();
    }

    private final void connect() {
        try {
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            //jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
            session=jsch.getSession(user, host, port);
            //session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
            session.setPassword(password);            
            session.setConfig(config);
            session.setTimeout(Integer.MAX_VALUE);
            session.connect();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(Integer.MAX_VALUE);
        } catch (Exception e) {
            tracer.error("SFTP Connection failed with exception : ", e);
        }
    }
    
    private final void disconnect() {
    	if (this.channel != null) {
    		if (this.channel.isConnected()) {
    			this.channel.disconnect();
    		}    		
    	}
    	
    	if (this.session != null) {
    		if (this.session.isConnected()) {
    			this.session.disconnect();
    		}
    	}
    }

    private final boolean isConnected() {
        if (session == null) {
            return false;
        }
        if(channel == null) {
            return false;
        }
        if (!session.isConnected()) {
            return false;
        }
        if (!channel.isConnected()) {
            return false;
        }
        if(channel.isClosed()) {
            return false;
        }
        return true;
    }

    private final String getRemoteTargetPathForArtifact(Path sourceArtifactPath, String targetArtifactName) {
        
    	//Path remoteTargetPath = Path.of(remoteDataDirectory, this.writeArchiveName, targetArtifactName);
        //Path.of replaces \ with / on windows. Instead just do the concatenation yourself using /
    	
    	String remoteTargetPath = remoteDataDirectory + "/" + this.writeArchiveName + "/" + targetArtifactName;
    	return remoteTargetPath;
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
                try {
                    createWriteArchiveIfNotExists();
                } catch (RemoteShippingException e) {
                    //Ignore this and still try the operation
                }
                String remoteTargetPath = getRemoteTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
                if (channel != null) {
	                channel.put(localTargetPath.toString(), remoteTargetPath.toString());
	                this.remoteWriteArchiveCreated = true;
	                localTargetPath.toFile().delete();
                } else {
                	throw new RemoteShippingException("Connection not established");
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
        if (sourceArtifactPath.toFile().exists() && !localTargetPath.toFile().exists()) {
            super.copyToWriteArchive(sourceArtifactPath, targetArtifactName);
        }
        try {
            if (localTargetPath.toFile().exists()) {
                if (!isConnected()) {
                    connect();
                }
                try {
                    createWriteArchiveIfNotExists();
                } catch(RemoteShippingException e) {
                    //Ignore and still try the operation
                }
                String remoteTargetPath = getRemoteTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
                if (channel != null) {
                	channel.put(localTargetPath.toString(), remoteTargetPath.toString());
                    this.remoteWriteArchiveCreated = true;
                } else {
                	throw new RemoteShippingException("Connection not established");
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
        if (remoteWriteArchiveCreated) {
            return;
        }
        if (!isConnected()) {
            connect();
        }
        super.createWriteArchiveIfNotExists();
        try {        	
            //channel.mkdir(Path.of(remoteDataDirectory, writeArchiveName).toString());      
        	//Below code works without throwing exception.
        	channel.cd(remoteDataDirectory);
        	channel.mkdir(writeArchiveName);        	
            this.remoteWriteArchiveCreated = true;
        } catch(Exception e) {
            //throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);
        	//Ignore this exception as this is supposed to throw exception due to permission restrictions.
        	
        }        
        /*
        //Check if directory is created and if yes set remoteArchiveCreated        
        try {
        	SftpATTRS attrs = channel.lstat(Path.of(remoteDataDirectory, writeArchiveName).toString());
        	if (attrs != null) {
        		this.remoteWriteArchiveCreated = true;
        	}
        } catch (Exception e) {
        	//Ignore
        	//throw new RemoteShippingException("Failed to create remote write archive with exception : ", e);        	
        }
        */
    }
    

    @Override
	List<Path> getObjectsInReadArchive() throws SQLException {
        if (!isConnected()) {
            connect();
        }
    	String remoteTargetPath = remoteCommandDirectory + "/" + this.readArchiveName;
    	try {
		     Vector<ChannelSftp.LsEntry> fileList = channel.ls(remoteTargetPath);
		     //List<String> commandFiles = new ArrayList<String>();
	         for (ChannelSftp.LsEntry entry : fileList) {
                if (!entry.getAttrs().isDir()) {
                	String dstFilePath = readArchivePath.resolve(entry.getFilename()).toString();
                	String srcFilePath = remoteTargetPath + "/" + entry.getFilename();
                	channel.get(srcFilePath, dstFilePath);
                	//commandFiles.add(entry.getFilename().toString());
                }
	         }					
			return super.getObjectsInReadArchive();
		} catch (Exception e) {
			throw new RemoteShippingException("Failed to lookup readArchive : " + readArchivePath + " from remote location : " + remoteTargetPath , e);
		}
	}

    @Override    
    void terminate() {
    	disconnect();
    }
}
