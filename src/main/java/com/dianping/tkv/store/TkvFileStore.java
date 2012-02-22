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

	private RandomAccessFile writeRAF;

	private RandomAccessFile readRAF;

	public TkvFileStore(File dbFile) throws IOException {
		writeRAF = new RandomAccessFile(dbFile, "rw");
		readRAF = new RandomAccessFile(dbFile, "r");
	}

	@Override
	public void append(byte b) throws IOException {
		writeRAF.seek(writeRAF.length());
		writeRAF.writeByte(b);
	}

	@Override
	public void append(byte[] bytes) throws IOException {
		writeRAF.seek(writeRAF.length());
		writeRAF.write(bytes);
	}

	@Override
	public void close() throws IOException {
		writeRAF.close();
		readRAF.close();
	}

	@Override
	public byte[] get(int pos, int size) throws IOException {
		byte[] bytes = new byte[size];
		writeRAF.seek(pos);
		writeRAF.read(bytes);
		return bytes;
	}

	@Override
	public long length() throws IOException {
		return writeRAF.length();
	}

}
