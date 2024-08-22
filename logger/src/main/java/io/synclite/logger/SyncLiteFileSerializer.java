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
