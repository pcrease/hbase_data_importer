package com.hbase.import_tool.spatial.main;

import java.util.ArrayList;

public class ColumnFamily {

	ArrayList<Column> columns = new ArrayList<Column>();
	String name;
	
	public ColumnFamily(ArrayList<Column> columns,
	String name){
		this.columns=columns;
		this.name=name;
	}
	
	public void addColumn(Column column){
		columns.add(column);
	}

}
