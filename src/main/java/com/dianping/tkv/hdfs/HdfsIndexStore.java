/**
 * 
 */
package com.dianping.tkv.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.dianping.tkv.IndexStore;
import com.dianping.tkv.Meta;
import com.dianping.tkv.local.RAFIndexStore;
import com.dianping.tkv.util.IoKit;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class HdfsIndexStore implements IndexStore {
	private RAFIndexStore localIndexStore;

	private FileSystem fs;

	private Path path;

	private File localFile;

	public HdfsIndexStore() {

	}

	public HdfsIndexStore(FileSystem fs, String hdfsFilename, File localFile, int keyLength, int tagLength) throws IOException {
		this.fs = fs;
		this.localFile = localFile;
		this.localIndexStore = new RAFIndexStore(localFile, keyLength, tagLength);
		this.path = new Path(fs.getWorkingDirectory(), hdfsFilename);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.IndexStore#append(com.dianping.tkv.Meta)
	 */
	@Override
	public void append(Meta meta) throws IOException {
		this.localIndexStore.append(meta);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.hdfs.IndexStore#close()
	 */
	@Override
	public void close() throws IOException {
		this.localIndexStore.close();
		this.fs.close();
	}

	@Override
	public boolean delete() throws IOException {
		boolean localDeleted = false;
		if (this.localIndexStore != null) {
			localDeleted = this.localIndexStore.delete();
		}
		boolean remoteDeleted = false;
		if (this.fs != null) {
			remoteDeleted = this.fs.delete(path, false);
		}
		return localDeleted && remoteDeleted;
	}

	public void download() throws IOException {
		InputStream input = fs.open(path);
		OutputStream output = new FileOutputStream(this.localFile);
		IoKit.copyAndClose(input, output);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.hdfs.IndexStore#getIndex(int)
	 */
	@Override
	public Meta getIndex(int indexPos) throws IOException {
		return this.localIndexStore.getIndex(indexPos);
	}

	@Override
	public Meta getIndex(String key) throws IOException {
		return this.localIndexStore.getIndex(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.hdfs.IndexStore#getIndex(java.lang.String)
	 */
	@Override
	public Meta getIndex(String key, Comparator<String> c) throws IOException {
		return this.localIndexStore.getIndex(key, c);
	}

	@Override
	public Meta getIndex(String key, String tagName) throws IOException {
		return this.localIndexStore.getIndex(key, tagName);
	}

	@Override
	public Meta getIndex(String key, String tagName, Comparator<String> c) throws IOException {
		return this.localIndexStore.getIndex(key, tagName, c);
	}

	@Override
	public int getIndexLength() {
		return this.localIndexStore.getIndexLength();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.dianping.tkv.hdfs.IndexStore#size()
	 */
	@Override
	public long size() throws IOException {
		return length() / getIndexLength();
	}

	@Override
	public void flush() throws IOException {
		InputStream input = new FileInputStream(this.localFile);
		OutputStream output = fs.create(path);
		IoKit.copyAndClose(input, output);
	}

	@Override
	public long length() throws IOException {
		return this.fs.getFileStatus(path).getLen();
	}

}
