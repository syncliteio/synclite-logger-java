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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class SyncLiteFileSerializer implements Serializer<Map<?,?>> {

	@Override
	public byte[] serialize(String topic, Map<?,?> data) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] stream = null;
		try {
			out = new ObjectOutputStream(baos);
			out.writeObject(data);
			stream = baos.toByteArray();
		} catch (IOException e) {	
			//Not handled here, will be handled in caller by checking bytes.
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				//Ignore here
			}
			try {
				baos.close();
			} catch (IOException ex) {
				// ignore here
			}
		}
		return stream;
	}
}
