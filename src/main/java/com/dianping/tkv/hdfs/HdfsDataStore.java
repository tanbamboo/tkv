/**
 * 
 */
package com.dianping.tkv.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dianping.tkv.DataStore;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class HdfsDataStore implements DataStore {
	private FileSystem fs;

	private Path path;

	private FSDataInputStream input;

	private FSDataOutputStream output;

	public HdfsDataStore(String dir, String filename) throws IOException {
		Configuration config = new Configuration();
		config.setInt("io.file.buffer.size", 8192);
		URI uri = URI.create(dir);
		FileSystem fs;
		if (uri == null) {
			fs = FileSystem.getLocal(config);
		} else {
			fs = FileSystem.get(uri, config);
		}
		this.fs = fs;
		path = new Path(fs.getWorkingDirectory(), filename);
		input = fs.open(path, 1024);
		output = fs.create(path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.DataStore#get(long, int)
	 */
	@Override
	public byte[] get(long offset, int length) throws IOException {
		byte[] bytes = new byte[length];
		synchronized (input) {
			input.seek(offset);
			input.read(bytes);
		}
		return bytes;
	}

	@Override
	public void append(long offset, byte[] bytes) throws IOException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.DataStore#append(byte[])
	 */
	@Override
	public void append(byte[] bytes) throws IOException {
		synchronized (output) {
			output.write(bytes);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.DataStore#append(byte)
	 */
	@Override
	public void append(byte b) throws IOException {
		synchronized (output) {
			output.write(b);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.DataStore#close()
	 */
	@Override
	public void close() throws IOException {
		synchronized (output) {
			this.output.close();
		}
		synchronized (input) {
			this.input.close();
		}
		this.fs.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.DataStore#length()
	 */
	@Override
	public long length() throws IOException {
		return this.fs.getFileStatus(path).getLen();
	}

}
