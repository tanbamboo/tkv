/**
 * 
 */
package com.dianping.tkv.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dianping.tkv.Meta;
import com.dianping.tkv.BatchHolder;
import com.dianping.tkv.StoreTestHelper;
import com.dianping.tkv.Tag;

/**
 * @author sean.wang
 * @since Mar 7, 2012
 */
public class HdfsImplTest extends StoreTestHelper {

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
		this.localIndexFile.delete();
		this.localDataFile.delete();
	}

	@Test
	public void testPutMetaHolder() throws IOException {
		final Meta meta1 = super.getMeta1();

		final Meta meta2 = super.getMeta2();

		final HdfsImpl hdfs = new HdfsImpl();
		final List<Meta> metas = new ArrayList<Meta>();
		hdfs.setDataStore(new HdfsDataStore(){

			@Override
			public void append(byte[] bytes) throws IOException {
			}
			
		});
		hdfs.setIndexStore(new HdfsIndexStore() {

			@Override
			public void append(Meta meta) throws IOException {
				metas.add(meta);
			}

		});
		hdfs.batchPut(new BatchHolder() {
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

			@Override
			public byte[] getValue(String key) {
				return new byte[]{'1'};
			}

		});

		Assert.assertEquals(Arrays.toString(new Meta[] { meta1, meta2 }), Arrays.toString(metas.toArray()));
		hdfs.delete();
	}

	@Test
	public void testPutAndGet() throws IOException {
		FileSystem localHdfsDir = HdfsHelper.createLocalFileSystem(super.localHdfsDir.getAbsolutePath());
		final HdfsImpl hdfs = new HdfsImpl(localHdfsDir, super.localDir, localIndexFile.getName(), localDataFile.getName(), 8, 100);
		Meta m1 = super.getMeta1();
		String value1 = "1234";
		hdfs.startWrite();
		Assert.assertTrue(hdfs.put(m1.getKey(), value1.getBytes()));
		hdfs.endWrite();
		hdfs.startRead();
		Assert.assertEquals(value1, new String(hdfs.get(m1.getKey())));
		hdfs.endRead();
		hdfs.close();
		hdfs.delete();
	}

}
