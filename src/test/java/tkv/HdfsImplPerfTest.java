package tkv;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tkv.HdfsHelper;
import tkv.HdfsImpl;

public class HdfsImplPerfTest extends StoreTestHelper {
	@Before
	public void tearUp() {
		this.localIndexFile.delete();
		this.localDataFile.delete();
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
	public void testConcurrentWriteRead() throws Exception {
		FileSystem localHdfsDir = HdfsHelper.createLocalFileSystem(super.localHdfsDir.getAbsolutePath());
		final HdfsImpl hdfs = new HdfsImpl(localHdfsDir, super.localDir, localIndexFile.getName(), localDataFile.getName(), 16, 100);

		hdfs.startWrite();

		ExecutorService pool = Executors.newFixedThreadPool(10);

		long start = System.currentTimeMillis();
		for (int p = 0; p < 10; p++) {
			final int num = p;

			pool.submit(new Runnable() {
				public void run() {
					try {
						for (int i = 0; i < 100; i++) {
							int seq = num * 100 + i;
							String id = "id" + seq;
							String t1 = "value" + seq;
							boolean success = hdfs.put(id, t1.getBytes());

							if (!success) {
								Assert.fail("Data failed to store at " + seq + ".");
							}
						}
					} catch (IOException e) {
						Assert.fail(e.getMessage());
					}
				}
			});
		}

		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		hdfs.buildIndex();
		hdfs.endWrite();
		System.out.println("finished concurrent write." + (System.currentTimeMillis() - start));

		hdfs.startRead();
		start = System.currentTimeMillis();
		for (int p = 0; p < 10; p++) {
			final int num = p;

			pool.submit(new Runnable() {
				public void run() {
					try {
						for (int i = 0; i < 100; i++) {
							int seq = num * 100 + i;
							String id = "id" + seq;
							String t1 = "value" + seq;
							String t2 = new String(hdfs.get(id));
							Assert.assertEquals("Unable to find data after stored it.", t1, t2);
						}
					} catch (IOException e) {
						Assert.fail(e.getMessage());
					}
				}
			});
		}

		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		System.out.println("finished concurrent read." + (System.currentTimeMillis() - start));
		hdfs.endRead();

		hdfs.close();
		hdfs.delete();
	}

	@Test
	public void testSerial() throws IOException {
		FileSystem localHdfsDir = HdfsHelper.createLocalFileSystem(super.localHdfsDir.getAbsolutePath());
		final HdfsImpl hdfs = new HdfsImpl(localHdfsDir, super.localDir, localIndexFile.getName(), localDataFile.getName(), 16, 100);

		hdfs.startWrite();

		long start = System.currentTimeMillis();
		for (int p = 0; p < 10; p++) {
			final int num = p;

			for (int i = 0; i < 100; i++) {
				int seq = num * 100 + i;
				String id = "id" + seq;
				String t1 = "value" + seq;
				String tag = "tag" + (i % 10);
				boolean success = hdfs.put(id, t1.getBytes(), tag);

				if (!success) {
					Assert.fail("Data failed to store at " + seq + ".");
				}
			}
		}

		hdfs.buildIndex();
		hdfs.endWrite();
		System.out.println("finished serial write." + (System.currentTimeMillis() - start));

		hdfs.startRead();

		// test serial read
		start = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			String id = "id" + i;
			String t1 = "value" + i;
			byte[] value = hdfs.get(id);
			if (value == null) {
				System.out.println(id);
			}
			Assert.assertEquals("Unable to find data after stored it." + id, t1, new String(value));
		}
		System.out.println("finished serial read." + (System.currentTimeMillis() - start));

		hdfs.endRead();

		hdfs.close();
		hdfs.delete();
	}

	@Test
	public void testSerialWithoutTag() throws IOException {
		FileSystem localHdfsDir = HdfsHelper.createLocalFileSystem(super.localHdfsDir.getAbsolutePath());
		final HdfsImpl hdfs = new HdfsImpl(localHdfsDir, super.localDir, localIndexFile.getName(), localDataFile.getName(), 16, 100);

		hdfs.startWrite();

		long start = System.currentTimeMillis();
		for (int p = 0; p < 10; p++) {
			final int num = p;

			for (int i = 0; i < 100; i++) {
				int seq = num * 100 + i;
				String id = "id" + seq;
				String t1 = "value" + seq;
				boolean success = hdfs.put(id, t1.getBytes());

				if (!success) {
					Assert.fail("Data failed to store at " + seq + ".");
				}
			}
		}

		hdfs.buildIndex();
		hdfs.endWrite();
		System.out.println("finished serial write." + (System.currentTimeMillis() - start));

		hdfs.startRead();

		// test serial read
		start = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			String id = "id" + i;
			String t1 = "value" + i;
			byte[] value = hdfs.get(id);
			if (value == null) {
				System.out.println(id);
			}
			Assert.assertEquals("Unable to find data after stored it." + id, t1, new String(value));
		}
		System.out.println("finished serial read." + (System.currentTimeMillis() - start));

		hdfs.endRead();

		hdfs.close();
		hdfs.delete();
	}

}
