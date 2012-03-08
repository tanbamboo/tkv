package com.dianping.tkv;

import java.io.IOException;
import java.util.Comparator;

public interface IndexStore {

	void append(Meta meta) throws IOException;

	void close() throws IOException;

	Meta getIndex(int indexPos) throws IOException;

	Meta getIndex(String key) throws IOException;

	Meta getIndex(String key, Comparator<String> c) throws IOException;

	Meta getIndex(String key, String tag) throws IOException;

	Meta getIndex(String key, String tag, Comparator<String> c) throws IOException;

	int size() throws IOException;

}
