/**
 * 
 */
package com.dianping.tkv.local;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.tkv.IndexStoreTestHelper;
import com.dianping.tkv.Meta;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class RAFIndexStoreTest extends IndexStoreTestHelper {
	private RAFIndexStore indexStore;

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
		indexStore = new RAFIndexStore(indexFile, 8, 100);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		indexStore.close();
		indexFile.delete();
	}

	@Test
	public void testAppendAndGetIndex() throws IOException {
		final Meta meta1 = getMeta1();

		final Meta meta2 = getMeta2();

		// index meta1
		this.indexStore.append(meta1);

		// index meta2
		this.indexStore.append(meta2);

		// assert meta1
		Meta index = indexStore.getIndex(meta1.getKey());
		Assert.assertEquals(meta1.toString(), index.toString());

		// assert meta2
		index = indexStore.getIndex(meta2.getKey());
		Assert.assertEquals(meta2.toString(), index.toString());

		// assert get meta1 by tag
		index = indexStore.getIndex(meta1.getKey(), meta1.getTags().keySet().iterator().next());
		Assert.assertEquals(meta1.toString(), index.toString());

	}

}
