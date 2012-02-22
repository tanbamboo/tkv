/**
 * 
 */
package com.dianping.tkv;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.dianping.tkv.store.TkvFileStore;
import com.dianping.tkv.store.TkvStore;
import com.dianping.tkv.store.util.StringKit;

/**
 * Tagged key-value store implement.
 * 
 * @author sean.wang
 * @since Feb 21, 2012
 */
public class TkvImpl implements Tkv {
	private static class IndexItem {
		private int pos;

		private int bodyLength;

		private Map<String, Integer> tagPosMap;

		IndexItem(int pos, int bodyLength) {
			this.pos = pos;
			this.bodyLength = bodyLength;
		}

		void addTagPos(String tagName, int pos) {
			if (tagPosMap == null) {
				tagPosMap = new HashMap<String, Integer>();
			}
			tagPosMap.put(tagName, pos);
		}

		Integer getTagPos(String tagName) {
			if (tagPosMap == null) {
				return null;
			}
			return tagPosMap.get(tagName);
		}
	}

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private TkvStore store;

	private Map<String, IndexItem> keyValueIndex;

	private Map<String, List<String>> tagListIndex;

	public TkvImpl(File dbFile) throws IOException {
		this.store = new TkvFileStore(dbFile);
		this.keyValueIndex = new HashMap<String, IndexItem>();
		this.tagListIndex = new HashMap<String, List<String>>();
		deserial();
	}

	private void append(byte[] bytes) throws IOException {
		this.store.append(bytes);
	}

	private void appendLength(int length) throws IOException {
		this.store.append(int2Bytes(length));
	}

	private int bytes2Int(byte[] bytes) {

		return (bytes[0] & 0xff) << 24 | (bytes[1] & 0xff) << 16 | (bytes[2] & 0xff) << 8 | (bytes[3] & 0xff);
	}

	@Override
	public void close() throws IOException {
		this.store.close();
	}

	private Record createNewRecord(String key, byte[] value, String... tags) {
		Record newRecord = new Record();
		newRecord.setKey(key);
		newRecord.setValue(value);
		if (tags != null) {
			newRecord.setTags(tags);
			int len = 0;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < tags.length; i++) {
				String tag = tags[i];
				sb.append(tag);
				len += tag.length();
				if (i != tags.length - 1) {
					sb.append(Record.TAG_SPLITER);
					len++;
				}
			}
			newRecord.setTagsLength(len);
			newRecord.setTagsString(sb.toString());
		}
		return newRecord;
	}

	protected void deserial() throws IOException {
		TkvStore store = this.store;
		int pos = 0;// record position
		while (pos < store.length()) {
			int keyLength = bytes2Int(store.get(pos, 4));
			pos += 4;
			int valueLength = bytes2Int(store.get(pos, 4));
			pos += 4;
			int tagsLength = bytes2Int(store.get(pos, 4));
			pos += 4;
			byte[] keyBuf = store.get(pos, keyLength);
			pos += keyLength;
			byte[] valueBuf = store.get(pos, valueLength);
			pos += valueLength;
			String key = new String(keyBuf);
			String[] tagArray = null;
			Record r = new Record();
			r.setPos(pos);
			r.setKey(key);
			r.setValue(valueBuf);
			if (tagsLength > 0) {
				byte[] tagsBuf = store.get(pos, tagsLength);
				pos += tagsLength;
				String tags = new String(tagsBuf);
				tagArray = StringKit.split(tags, Record.TAG_SPLITER);
				r.setTags(tagArray);
				r.setTagsLength(tagsLength);
				r.setTagsString(tags);
			}
			index(r);
			pos += 1; // skip next
		}

	}

	@Override
	public byte[] get(String key) throws IOException {
		Record r;
		try {
			lock.readLock().lock();
			r = getRecord(null, key);
		} finally {
			lock.readLock().unlock();
		}
		return r.getValue();
	}

	public TkvStore getStore() {
		return this.store;
	}

	@Override
	public Record getRecord(String tagName, String key) throws IOException {
		Record r = new Record();
		byte[] body = null;
		IndexItem indexItem = null;
		try {
			lock.readLock().lock();
			indexItem = this.keyValueIndex.get(key);
			if (indexItem == null) {
				return null;
			}
			body = this.store.get(indexItem.pos, indexItem.bodyLength);
		} finally {
			lock.readLock().unlock();
		}
		byte[] intBuf = new byte[4];
		System.arraycopy(body, 0, intBuf, 0, intBuf.length);
		int keyLength = bytes2Int(intBuf);
		System.arraycopy(body, 4, intBuf, 0, intBuf.length);
		byte[] keyBuf = new byte[keyLength];
		System.arraycopy(body, 12, keyBuf, 0, keyLength);
		r.setKey(new String(keyBuf));
		int valueLength = bytes2Int(intBuf);
		byte[] valueBuf = new byte[valueLength];
		System.arraycopy(body, 12 + keyLength, valueBuf, 0, valueLength);
		r.setValue(valueBuf);
		if (tagName != null) {
			Integer pos = indexItem.getTagPos(tagName);
			if (pos != null) {
				List<String> tagList = this.tagListIndex.get(tagName);
				String nextKey = pos == tagList.size() - 1 ? null : tagList.get(pos + 1);
				String priviousKey = pos == 0 ? null : tagList.get(pos - 1);
				r.setPriviousKey(priviousKey);
				r.setNexKey(nextKey);
			}
		}
		return r;
	}

	/**
	 * @param pos
	 * @param keyLength
	 * @param valueLength
	 * @param tagsLength
	 * @param tagArray
	 * @param key
	 */
	private void index(Record record) {
		String key = record.getKey();
		IndexItem item = new IndexItem(record.getPos(), record.getBodyLength());
		this.keyValueIndex.put(key, item);
		String[] tagArray = record.getTags();
		if (tagArray != null) {
			for (String tag : tagArray) {
				List<String> list = this.tagListIndex.get(tag);
				if (list == null) {
					list = new LinkedList<String>();
					this.tagListIndex.put(tag, list);
				}
				list.add(key);
				item.addTagPos(tag, list.size() - 1);
			}
		}
	}

	public byte[] int2Bytes(int num) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) (num >>> 24);
		bytes[1] = (byte) (num >>> 16);
		bytes[2] = (byte) (num >>> 8);
		bytes[3] = (byte) num;
		return bytes;
	}

	@Override
	public void put(String key, byte[] value) throws IOException {
		put(key, value, (String[]) null);
	}

	@Override
	public void put(String key, byte[] value, String... tags) throws IOException {
		try {
			lock.writeLock().lock();
			Record newRecord = createNewRecord(key, value, tags);
			storeNewRecord(newRecord); // store and set pos
			index(newRecord);
		} finally {
			lock.writeLock().unlock();
		}

	}

	private void putEnder() throws IOException {
		this.store.append((byte) Record.ENDER);
	}

	@Override
	public int size() {
		return this.keyValueIndex.size();
	}

	private void storeNewRecord(Record newRecord) throws IOException {
		newRecord.setPos((int) store.length());
		appendLength(newRecord.getKey().length());
		appendLength(newRecord.getValue().length);
		appendLength(newRecord.getTagsLength());
		append(newRecord.getKey().getBytes());
		append(newRecord.getValue());
		if (newRecord.getTags() != null) {
			append(newRecord.getTagsToString().getBytes());
		}
		putEnder();
	}
}
