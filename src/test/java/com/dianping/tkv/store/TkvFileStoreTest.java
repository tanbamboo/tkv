package com.dianping.tkv.store;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.tkv.TkvImpl;
import com.dianping.tkv.store.TkvFileStore;

public class TkvFileStoreTest {
	File dbFile;
	TkvImpl tkv;
	private TkvFileStore store;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		dbFile = new File("/tmp/tkvtest.db");
		tkv = new TkvImpl(dbFile);
		store = (TkvFileStore) tkv.getStore();
	}

	@After
	public void tearDown() throws Exception {
		store.close();
		tkv.close();
		dbFile.delete();
	}

	@Test
	public void testGet() throws IOException {
		String key = "01234567";
		String value = "ayellowdog";
		String[] tags = new String[] { "dog", "pet" };
		tkv.put(key, value.getBytes(), tags);
		Assert.assertEquals(4 + 4 + 4 + key.length() + value.length(), +7 + 1, dbFile.length());
		TkvStore store = tkv.getStore();
		// assert store
		Assert.assertEquals(key.length(), bytes2Int(store.get(0, 4)));
		Assert.assertEquals(value.length(), bytes2Int(store.get(4, 4)));
		Assert.assertEquals(7, bytes2Int(store.get(8, 4)));
		Assert.assertEquals(key, new String(store.get(12, key.length())));
		Assert.assertEquals(value, new String(store.get(12 + key.length(), value.length())));
		Assert.assertEquals("dog\tpet", new String(store.get(12 + key.length() + value.length(), 7)));
		Assert.assertEquals("\n", new String(store.get(12 + key.length() + value.length() + 7, 1)));
		// assert tkv
		Assert.assertEquals(value, new String(tkv.get(key)));
	}

	private int bytes2Int(byte[] bytes) {
		return (bytes[0] & 0xff) << 24 | (bytes[1] & 0xff) << 16 | (bytes[2] & 0xff) << 8 | (bytes[3] & 0xff);
	}

}
