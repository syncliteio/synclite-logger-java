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

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import org.apache.log4j.Logger;

class FSArchiver {
	protected final Path dbPath;
	protected final String writeArchiveName;
	protected final Path writeArchivePath;
	protected final String readArchiveName;
	protected final Path readArchivePath;
	protected final Logger tracer;
	private final Path encryptionKeyFile;
	private final FileCopier fileCopier;

	private abstract class FileCopier {
		protected abstract void moveFile(Path sourcePath, Path targetPath) throws SQLException;
		protected abstract void copyFile(Path sourcePath, Path targetPath) throws SQLException;
	}

	private class PlainFileCopier extends FileCopier {    	
		@Override
		protected void moveFile(Path sourcePath, Path targetPath) throws SQLException {
			try {
				//
				//Move as COPY + DELETE is safer to do for idempotency
				Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
				try {
					Files.delete(sourcePath);
				} catch(IOException e) {
					//Ignore if unable to delete the source file
					tracer.error("FileCopier failed to delete file after shipping : " + sourcePath, e);
				}				
				//Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
			} catch(IOException e) {
/*				if (e instanceof NoSuchFileException) {
					//This is for idempontency
					//Check if target file is present and source file is not and that means the move was already done, 
					//we can ignore this error and move on.
					//
					if (Files.exists(targetPath) && ! Files.exists(sourcePath)) {
						//Ignore.
					} else {
						throw new SQLException("FileCopier failed to move file src :" + sourcePath + ", dst : " + targetPath, e);
					}
				} else  {
					throw new SQLException(e);
				}				
*/
				throw new SQLException(e);
			}
		}

		@Override
		protected void copyFile(Path sourcePath, Path targetPath) throws SQLException {
			try {
				Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
			} catch(IOException e) {
				throw new SQLException(e);
			}
		}
	}

	private class EncryptedFileCopier extends FileCopier {
		private static final int ENCRYPTION_BLOCK_SIZE = 2048;

		@Override
		protected void moveFile(Path sourcePath, Path targetPath) throws SQLException {    		
			copyFile(sourcePath, targetPath);
			//Delete source file
			try {
				Files.delete(sourcePath);
			} catch(IOException e) {
				throw new SQLException("Failed to delete source file after ecnrypting : " + sourcePath, e);
			}
		}

		private final PublicKey readPubKeyFromFile() throws SQLException{
			try {
				byte[] bytes = Files.readAllBytes(encryptionKeyFile);
				X509EncodedKeySpec keySpec = new X509EncodedKeySpec(bytes);
				KeyFactory keyFactory = KeyFactory.getInstance("RSA");
				PublicKey publicKey = keyFactory.generatePublic(keySpec);
				return publicKey;
			} catch(IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
				throw new SQLException("Failed to load public key from the specified file : " + encryptionKeyFile, e);
			}
		}

		private final byte[] encryptKey(byte[] data) throws SQLException {
			try {
				PublicKey pubKey = readPubKeyFromFile();
				Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
				cipher.init(Cipher.ENCRYPT_MODE, pubKey);
				byte[] encryptedData = cipher.doFinal(data); 
				return encryptedData;
			} catch(InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | SQLException | IllegalBlockSizeException | BadPaddingException e) {
				throw new SQLException("Failed to encrypt key ", e);
			}
		}

		private final Key generateEncryptionKey() throws SQLException {
			KeyGenerator keyGen;
			try {
				keyGen = KeyGenerator.getInstance("AES");
				keyGen.init(256); // 256-bit key
				SecretKey secretKey = keyGen.generateKey();
				return secretKey ;
			} catch (NoSuchAlgorithmException e) {
				throw new SQLException("SyncLite log shipper failed to generate an encryption key", e);
			}
		}


		@Override
		protected void copyFile(Path sourcePath, Path targetPath) throws SQLException {
			//Read contents of sourcePath, encrypt and write out to targetPath.
			//Delete sourcePath

			try {
				if (Files.exists(targetPath)) {
					Files.delete(targetPath);
				}
			} catch (IOException e) { 
				throw new SQLException("Failed to delete already existing targetPath : " + targetPath);
			}

			try {
				Files.createFile(targetPath);	
			} catch (IOException e) { 
				throw new SQLException("Failed to create a file at targetPath : " + targetPath);
			}

			Key encryptionKey = generateEncryptionKey();

			//Encrypt the encryption key with pub key    		
			byte[] encryptedEncryptionKey = encryptKey(encryptionKey.getEncoded());

	        
			Cipher cipher;
			byte[] iv;
			try {
				cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
				IvParameterSpec ivParams;
				//Generate IV
				iv = new byte[cipher.getBlockSize()];
				SecureRandom random = new SecureRandom();
				random.nextBytes(iv);
				ivParams = new IvParameterSpec(iv);
				cipher.init(Cipher.ENCRYPT_MODE, encryptionKey, ivParams);

			} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
				throw new SQLException("Failed to create a Cipher for encryption", e);
			}


			try (FileInputStream in = new FileInputStream(sourcePath.toString())) {
				try (FileOutputStream out = new FileOutputStream(targetPath.toString())) {
					try (DataOutputStream dos = new DataOutputStream(out)) {
						// Encrypt the input file and write the output to the output file
						byte[] buffer = new byte[ENCRYPTION_BLOCK_SIZE];
						int bytesRead;
						//Write out encrypted encryption key first up
						dos.writeInt(encryptedEncryptionKey.length);
						dos.write(encryptedEncryptionKey);
						dos.writeInt(iv.length);
						dos.write(iv);
						while ((bytesRead = in.read(buffer)) != -1) {
							byte[] encryptedBytes = cipher.update(buffer, 0, bytesRead);
							if (encryptedBytes != null) {
								dos.write(encryptedBytes);
							}
						}
						byte[] finalBytes = cipher.doFinal();
						dos.write(finalBytes);
						dos.flush();
					}
				}
			} catch (IOException | IllegalBlockSizeException | BadPaddingException e) {
				throw new SQLException("Failed to encrypt and copy source file : " + sourcePath + " to target path :" + targetPath, e);
			}    		
		}
	}

	FSArchiver(Path dbPath, String writArchiveName, Path writeArchivePath, String readArchiveName, Path readArchivePath, Logger tracer, Path encryptionKeyPath) {
		this.dbPath = dbPath;
		this.writeArchiveName = writArchiveName;
		this.writeArchivePath = writeArchivePath;
		this.readArchiveName = readArchiveName;
		this.readArchivePath = readArchivePath;
		this.tracer = tracer;
		this.encryptionKeyFile = encryptionKeyPath;
		if (encryptionKeyPath != null) {
			this.fileCopier = new EncryptedFileCopier();
		} else {
			this.fileCopier = new PlainFileCopier();
		}
	}

	protected final Path getTargetPathForArtifact(Path sourceArtifactPath, String targetArtifactName) {
		Path targetPath = Path.of(this.writeArchivePath.toString(), targetArtifactName);
		return targetPath;
	}

	protected final void doMoveToWriteArchive(Path sourcePath, Path targetPath) throws SQLException {
		//Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
		fileCopier.moveFile(sourcePath, targetPath);
	}

	protected final void doCopyToWriteArchive(Path sourcePath, Path targetPath) throws SQLException {
		//Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
		fileCopier.copyFile(sourcePath, targetPath);
	}

	void moveToWriteArchive(Path sourceArtifactPath, String targetArtifactName) throws SQLException {
		synchronized(this) {
			Path targetPath = getTargetPathForArtifact(sourceArtifactPath, targetArtifactName);
			try {
				doMoveToWriteArchive(sourceArtifactPath, targetPath);
			} catch (SQLException e) {
				//If the file is already moved to targetPath here then just move on as there is nothing to do.
				//if ( !Files.exists(targetPath)) {
				//	throw new SQLException("SyncLite FSArchiver failed to move " + sourceArtifactPath + " to archive path : " + writeArchivePath.toString(), e);
				//}
				throw new SQLException("SyncLite FSArchiver failed to move " + sourceArtifactPath + " to archive path : " + writeArchivePath.toString(), e);
			}
		}
	}

	void copyToWriteArchive(Path sourceArtifactPath, String newArtifactName) throws SQLException {
		synchronized(this) {
			try {
				Path targetPath = getTargetPathForArtifact(sourceArtifactPath, newArtifactName);
				doCopyToWriteArchive(sourceArtifactPath, targetPath);
			} catch (SQLException e) {
				//e.printStackTrace();
				throw new SQLException("SyncLite FSArchiver failed to copy " + sourceArtifactPath + " to archive path : " + writeArchivePath.toString(), e);
			}
		}
	}


	void deleteLocalObject(Path localObject) throws SQLException {
		try {
			if (Files.exists(localObject)) {
				Files.delete(localObject);
			}
		} catch(Exception e) {
			tracer.trace("Failed to delete local object : " + localObject + " : " + e.getMessage(), e);
		}
	}
	
	void createWriteArchiveIfNotExists() throws SQLException {
		createLocalWriteArchiveIfNotExists();
	}

	boolean archiveExists() {
		return localArchiveExists();
	}
	
	boolean localArchiveExists() {
		return Files.exists(this.writeArchivePath);
	}

	void createLocalWriteArchiveIfNotExists() throws SQLException {
		synchronized (this) {
			if (!localArchiveExists()) {
				try {
					Files.createDirectories(writeArchivePath);
				} catch (IOException e) {
					tracer.error("SyncLite FSArchiver failed to create directory : " + writeArchivePath.toString() + " : " + e.getMessage(), e);
					throw new SQLException("SyncLite FSArchiver failed to create directory : " + writeArchivePath.toString() + " : " + e.getMessage(), e);
				}
			}
		}
	}

	void terminate() {		
		//Nothing to do here.
	}
	
	List<Path> getObjectsInReadArchive() throws SQLException {
		try {
			List<Path> commandFiles = Files.walk(this.readArchivePath).filter(f -> !f.equals(this.readArchivePath)).collect(Collectors.toList());
			return commandFiles;		
		} catch (IOException e) {
			if (e instanceof NoSuchFileException) {
				//readArchive may not have been created yet. Ignore this error and return empty list
			} else {
				throw new SQLException("Failed to lookup readArchive : " + readArchivePath, e);
			}
		}
		return Collections.EMPTY_LIST;
	}

}
