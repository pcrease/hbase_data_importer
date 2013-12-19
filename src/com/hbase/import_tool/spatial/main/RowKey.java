package com.hbase.import_tool.spatial.main;

import java.util.ArrayList;

public class RowKey {

	boolean isComposite;
	boolean hasSpatialIndex;
	ArrayList<String> sourceFields = new ArrayList<String>();
	String delimiter;

	public RowKey(boolean isComposite, boolean hasSpatialIndex,
			ArrayList<String> sourceFields, String delimiter) {
		this.isComposite = isComposite;
		this.hasSpatialIndex=hasSpatialIndex;
		this.sourceFields=sourceFields;
		this.delimiter=delimiter;
	}

}
