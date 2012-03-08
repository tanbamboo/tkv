/**
 * 
 */
package com.dianping.tkv.hdfs;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.tkv.IndexStore;
import com.dianping.tkv.Meta;
import com.dianping.tkv.Tag;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class HdfsIndexStoreTest {
	private IndexStore indexStore;

	private File indexFile = new File(System.getProperty("user.dir") + "/target/index_test");

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		indexFile.delete();
		indexStore = new HdfsIndexStore(null, indexFile, 8, 100);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		indexFile.delete();
	}

	@Test
	public void testAppendAndGetIndex() throws IOException {
		final Meta meta1 = new Meta();
		meta1.setKey("12345678");
		meta1.setOffset(0);
		meta1.setLength(10);
		Tag t1 = new Tag();
		t1.setName("pet");
		t1.setNext(1);
		meta1.addTag(t1);
		Tag t2 = new Tag();
		t2.setName("bird");
		meta1.addTag(t2);

		// index meta1
		this.indexStore.append(meta1);

		final Meta meta2 = new Meta();
		meta2.setKey("87654321");
		meta2.setOffset(10);
		meta2.setLength(20);
		Tag t3 = new Tag();
		t3.setName("pet");
		t3.setPrevious(0);
		meta2.addTag(t3);

		// index meta2
		this.indexStore.append(meta2);

		// assert meta1
		Meta index = indexStore.getIndex(meta1.getKey());
		Assert.assertEquals(meta1.toString(), index.toString());

		// assert meta2
		index = indexStore.getIndex(meta2.getKey());
		Assert.assertEquals(meta2.toString(), index.toString());

		// assert get meta1 by tag
		index = indexStore.getIndex(meta1.getKey(), t1.getName());
		Assert.assertEquals(meta1.toString(), index.toString());

	}

}
