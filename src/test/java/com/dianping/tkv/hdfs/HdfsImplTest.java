/**
 * 
 */
package com.dianping.tkv.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.tkv.IndexStore;
import com.dianping.tkv.Meta;
import com.dianping.tkv.MetaHolder;
import com.dianping.tkv.Tag;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class HdfsImplTest {

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
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPutMetaHolder() throws IOException {
		final Meta meta1 = new Meta();
		meta1.setKey("12345678");
		meta1.setOffset(0);
		meta1.setLength(10);
		Tag t1 = new Tag();
		t1.setName("pet");
		t1.setNext(1);
		t1.setPos(0);
		meta1.addTag(t1);
		Tag t2 = new Tag();
		t2.setName("bird");
		t2.setPos(0);
		meta1.addTag(t2);

		final Meta meta2 = new Meta();
		meta2.setKey("87654321");
		meta2.setOffset(10);
		meta2.setLength(20);
		Tag t3 = new Tag();
		t3.setName("pet");
		t3.setPrevious(0);
		t3.setPos(1);
		meta2.addTag(t3);

		final HdfsImpl hdfs = new HdfsImpl();
		final List<Meta> metas = new ArrayList<Meta>();
		hdfs.setIndexStore(new IndexStore() {

			@Override
			public void append(Meta meta) throws IOException {
				metas.add(meta);
			}

			@Override
			public void close() throws IOException {
			}

			@Override
			public Meta getIndex(int indexPos) throws IOException {
				return null;
			}

			@Override
			public Meta getIndex(String key) throws IOException {
				return null;
			}

			@Override
			public Meta getIndex(String key, Comparator<String> c) throws IOException {
				return null;
			}

			@Override
			public Meta getIndex(String key, String tag) throws IOException {
				return null;
			}

			@Override
			public Meta getIndex(String key, String tag, Comparator<String> c) throws IOException {
				return null;
			}

			@Override
			public long size() throws IOException {
				return 0;
			}

			@Override
			public boolean delete() throws IOException {
				return false;
			}

			@Override
			public int getIndexLength() {
				return 0;
			}

			@Override
			public long length() throws IOException {
				return 0;
			}
		});
		hdfs.putIndex(new MetaHolder() {
			Map<String, Meta> map = new HashMap<String, Meta>();
			{
				map.put(meta1.getKey(), meta1);
				map.put(meta2.getKey(), meta2);
			}

			@Override
			public Collection<String> getKeys() {
				return Arrays.asList(meta2.getKey(), meta1.getKey());
			}

			@Override
			public void getMeta(String key, Meta meta) {
				Meta o = map.get(key);
				meta.setOffset(o.getOffset());
				meta.setLength(o.getLength());
				if (o.getTags() != null) {
					for (Tag t : o.getTags().values()) {
						meta.addTag(t.getName());
					}
				}
			}

		});

		Assert.assertEquals(Arrays.toString(new Meta[] { meta1, meta2 }), Arrays.toString(metas.toArray()));

	}

}
