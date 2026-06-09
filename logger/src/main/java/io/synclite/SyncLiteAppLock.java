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

package io.synclite;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;

public class SyncLiteAppLock {
	
	private FileChannel lockChannel;
	private FileLock lock;
	private Path lockFile;
	public SyncLiteAppLock() {
	}
	
	public final void tryLock(Path dbPath) throws SQLException {		
		this.lockFile = Path.of(dbPath.toString() + ".synclite", dbPath.getFileName().toString() + ".lock");
		try {
			this.lockChannel = FileChannel.open(this.lockFile,
					StandardOpenOption.CREATE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE);
			this.lock = this.lockChannel.tryLock();
			if (this.lock == null) {
				String existingOwner = readLockOwnerInfo();
				throw new SQLException("Failed to lock db file " + dbPath + ". Another application is using this db file"
						+ ". Existing lock owner details: " + existingOwner);
			}

			String ownerInfo = buildLockOwnerInfo();
			this.lockChannel.truncate(0);
			this.lockChannel.position(0);
			this.lockChannel.write(ByteBuffer.wrap(ownerInfo.getBytes(StandardCharsets.UTF_8)));
			this.lockChannel.force(true);
		} catch (OverlappingFileLockException e) {
			String existingOwner = readLockOwnerInfo();
			throw new SQLException("Failed to lock db file " + dbPath + ". Another application is using this db file"
					+ ". Existing lock owner details: " + existingOwner);
		} catch (Exception e) {
			release();
			throw new SQLException("Failed to lock db file " + dbPath + ". Another application is using this db file", e);
		}
	}

	public final void release() {
		if (this.lock != null) {
			try {
				this.lock.release();
			} catch (Exception e) {
				// Ignore lock release failures during shutdown.
			}
			this.lock = null;
		}

		if (this.lockChannel != null) {
			try {
				this.lockChannel.close();
			} catch (Exception e) {
				// Ignore channel close failures during shutdown.
			}
			this.lockChannel = null;
		}
	}

	private String buildLockOwnerInfo() {
		long pid = ProcessHandle.current().pid();
		String hostName;
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			hostName = "unknown-host";
		}

		StringBuilder info = new StringBuilder();
		info.append("pid=").append(pid);
		info.append(";host=").append(hostName);
		info.append(";timestamp=").append(Instant.now());
		return info.toString();
	}

	private String readLockOwnerInfo() {
		if (this.lockFile == null) {
			return "unavailable";
		}

		try {
			if (!java.nio.file.Files.exists(this.lockFile)) {
				return "unavailable";
			}
			String content = java.nio.file.Files.readString(this.lockFile, StandardCharsets.UTF_8).trim();
			if (content.isEmpty()) {
				return "unavailable";
			}
			return content;
		} catch (IOException e) {
			return "unavailable";
		}
	}
}
