/**
 * 
 */
package com.dianping.tkv.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author sean.wang
 * @since Nov 16, 2011
 */
public class TkvFileStore implements TkvStore {

	private RandomAccessFile access;

	public TkvFileStore(File dbFile) throws IOException {
		access = new RandomAccessFile(dbFile, "rw");
	}

	@Override
	public void append(byte b) throws IOException {
		access.seek(access.length());
		access.writeByte(b);
	}

	@Override
	public void append(byte[] bytes) throws IOException {
		access.seek(access.length());
		access.write(bytes);
	}

	@Override
	public void close() throws IOException {
		access.close();
	}

	@Override
	public byte[] get(int pos, int size) throws IOException {
		byte[] bytes = new byte[size];
		access.seek(pos);
		access.read(bytes);
		return bytes;
	}

	@Override
	public long length() throws IOException {
		return access.length();
	}

}
