/**
 * 
 */
package com.dianping.tkv;

import java.io.File;

/**
 * @author sean.wang
 * @since Mar 9, 2012
 */
public abstract class StoreTestHelper {

	protected File localDir = new File(System.getProperty("user.dir") + "/target/local/");

	protected File localHdfsDir = new File(System.getProperty("user.dir") + "/target/hdfs/");

	protected File localIndexFile = new File(localDir, "a.index");

	protected File localDataFile = new File(localDir, "a.data");

	public Meta getMeta1() {
		final Meta meta1 = new Meta();
		meta1.setKey("12345678");
		meta1.setOffset(0);
		meta1.setLength(10);
		Tag t1 = new Tag();
		t1.setName("pet");
		t1.setPos(0);
		t1.setNext(1);
		meta1.addTag(t1);
		Tag t2 = new Tag();
		t2.setName("bird");
		t2.setPos(0);
		meta1.addTag(t2);
		return meta1;
	}

	public Meta getMeta2() {
		final Meta meta2 = new Meta();
		meta2.setKey("87654321");
		meta2.setOffset(10);
		meta2.setLength(20);
		Tag t3 = new Tag();
		t3.setName("pet");
		t3.setPos(1);
		t3.setPrevious(0);
		meta2.addTag(t3);
		return meta2;
	}
}
