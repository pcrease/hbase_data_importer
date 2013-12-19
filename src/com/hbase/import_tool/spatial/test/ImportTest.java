package com.hbase.import_tool.spatial.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hbase.import_tool.spatial.main.HbaseDataModel;

public class ImportTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		HbaseDataModel hbaseDataModel= new HbaseDataModel();
		hbaseDataModel
				.generateModelFromConfigFile(("test_config_data/document.json"));
		
		Configuration configuration = HBaseConfiguration.create(new Configuration());
		
		//hbaseDataModel.createTableFromDataModel(configuration);
		hbaseDataModel.createTableFromDataModelAndImportData(configuration);

	}

}
