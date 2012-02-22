/**
 * 
 */
package com.dianping.tkv;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author sean.wang
 * @since Feb 20, 2012
 */
public class TkvPerfTest {

	private TkvImpl fkv;

	private File dbFile;

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
		dbFile = new File("/tmp/fkvtest.db");
		dbFile.delete();
		fkv = new TkvImpl(dbFile);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		fkv.close();
		dbFile.delete();
	}

	private int perfTimes = 100000;

	/**
	 * Test method for {@link com.dianping.tkv.TkvImpl#get(java.lang.String)}.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testPutDiffKeyWithoutTagsPerf() throws IOException {
		String value = "0123456789";
		long start = System.currentTimeMillis();
		for (int i = 0; i < perfTimes; i++) {
			fkv.put("" + (10000000 + i), value.getBytes());
		}
		System.out.println("testPutDiffKeyWithoutTagsPerf:" + (System.currentTimeMillis() - start));
	}

	@Test
	public void testGetDiffKeyWithoutTagsPerf() throws IOException {
		String value = "0123456789";
		for (int i = 0; i < perfTimes; i++) {
			fkv.put("" + (10000000 + i), value.getBytes());
		}
		long start = System.currentTimeMillis();
		for (int i = 0; i < perfTimes; i++) {
			fkv.get("" + (10000000 + i));
		}
		System.out.println("testGetDiffKeyWithoutTagsPerf:" + (System.currentTimeMillis() - start));
	}

	@Test
	public void testGetTagRecordPerf() throws IOException {
		String value = "0123456789";
		for (int i = 0; i < perfTimes; i++) {
			fkv.put("" + (10000000 + i), value.getBytes(), "pet" + i % 100);
		}
		long start = System.currentTimeMillis();
		for (int i = 0; i < perfTimes; i++) {
			fkv.getRecord("pet" + i % 100, "" + (10000000 + i));
		}
		System.out.println("testGetTagRecordPerf:" + (System.currentTimeMillis() - start));
	}

}
