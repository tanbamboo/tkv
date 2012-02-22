package com.dianping.tkv.store;

import java.io.IOException;

/**
 * @author sean.wang
 * @since Nov 16, 2011
 */
public interface TkvStore {
	byte[] get(int startIndex, int size) throws IOException;

	void append(byte[] bytes) throws IOException;
	
	void append(byte b) throws IOException;

	void close() throws IOException;

	long length() throws IOException;

}
