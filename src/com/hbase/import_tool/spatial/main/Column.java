package com.hbase.import_tool.spatial.main;

public class Column {

	String name;
	String type;
	String sourceField;
	
	public Column(String name,
	String sourceField,
	String type){
		this.name=name;
		this.type=type;
		this.sourceField=sourceField;
		
	}
}
