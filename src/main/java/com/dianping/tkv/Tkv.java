/**
 * 
 */
package com.dianping.tkv;

import java.io.IOException;

/**
 * Tagged key-value store interface.
 * 
 * @author sean.wang
 * @since Feb 21, 2012
 */
public interface Tkv {

	/**
	 * close fkv
	 * 
	 * @throws IOException
	 */
	void close() throws IOException;

	/**
	 * get record by key
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	byte[] get(String key) throws IOException;

	/**
	 * get tag by tag name
	 * 
	 * @return
	 * @throws IOException
	 */
	Record getRecord(String tagName, String key) throws IOException;

	/**
	 * put record without tags
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	void put(String key, byte[] value) throws IOException;

	/**
	 * put record with tags
	 * 
	 * @param key
	 * @param value
	 * @param tags
	 * @throws IOException
	 */
	void put(String key, byte[] value, String... tags) throws IOException;

	/**
	 * record size
	 * 
	 * @return
	 */
	int size();

}
