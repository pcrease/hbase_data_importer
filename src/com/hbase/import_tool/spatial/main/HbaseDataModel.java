package com.hbase.import_tool.spatial.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.HostFileManager.EntrySet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class HbaseDataModel {

    private final String[] SALT_PREFIX_ALPHABETLETTERS = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
            "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };

    private String tableName;
    private String pathToData;
    private boolean createNew;
    private RowKey rowKey;
    private ArrayList<ColumnFamily> columnFamilies = new ArrayList<ColumnFamily>();
    private HashMap<String, String> constants = new HashMap<String, String>();
    private HashMap<String, String> specialValues = new HashMap<String, String>();
    private boolean hasSalting;
    private String saltingMethod;

    private SimpleDateFormat transformTimeDataFormat;
    private TimeZone sourceTimeZone;
    private TimeZone targetTimeZone;
    private String timeTransformationDataField;
    private boolean timeConversion = false;

    private Random randomGenerator = new Random();
    // TODO add the functionality in
    private String insertionDateSpecialValue = "<currentTime>";

    public void generateModelFromConfigFile(String filePath) throws IOException, ParseException {
        String jsonString = "";

        // use special values
        specialValues.put(insertionDateSpecialValue, insertionDateSpecialValue);

        // read json file
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            jsonString = sb.toString();
        } finally {
            br.close();
        }

        // get object
        JSONObject obj = (JSONObject) JSONValue.parse(jsonString);
        // get table
        JSONObject tableData = (JSONObject) obj.get("table");
        // get values for table
        tableName = tableData.get("name").toString();
        pathToData = tableData.get("source_data").toString();

        // get constants
        JSONObject constantsObject = (JSONObject) tableData.get("constants");
        // only strings supported at the moment, but possible for more to be included
        Iterator it = constantsObject.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            constants.put((String) pairs.getKey(), (String) pairs.getValue());
        }

        // generate rowkey and get data for it
        JSONObject rowKeyObject = (JSONObject) tableData.get("rowkey");

        boolean isComposite = (Boolean) rowKeyObject.get("isComposite");
        boolean hasSpatiaIndex = (Boolean) rowKeyObject.get("hasSpatialIndex");
        hasSalting = (Boolean) rowKeyObject.get("hasSalting");

        if (hasSalting) {
            saltingMethod = (String) rowKeyObject.get("saltingMethod");
        }

        String sourceFieldsObj = (String) rowKeyObject.get("source_fields");

        ArrayList<String> sourceFields = new ArrayList<String>();
        for (String str : sourceFieldsObj.split(",")) {
            sourceFields.add(str);
        }
        String delimiter = (String) rowKeyObject.get("delimiter");

        rowKey = new RowKey(isComposite, hasSpatiaIndex, sourceFields, delimiter);

        JSONArray transformations = (JSONArray) tableData.get("transformations");
        
        if (transformations != null) {
            for (int j = 0; j < transformations.size(); j++) {
                JSONObject object = (JSONObject) transformations.get(j);
                if (object.containsKey("timezone_conversion")) {
                    timeConversion = true;
                    JSONObject timeToUTC = (JSONObject) object.get("timezone_conversion");
                    String format = (String) timeToUTC.get("format");
                    String source_timezone = (String) timeToUTC.get("source_timezone");
                    String target_timezone = (String) timeToUTC.get("target_timezone");
                    timeTransformationDataField = (String) timeToUTC.get("hbase_data_fieldname");

                    transformTimeDataFormat = new SimpleDateFormat(format);
                    sourceTimeZone = TimeZone.getTimeZone(source_timezone);
                    transformTimeDataFormat.setTimeZone(sourceTimeZone);
                    targetTimeZone = TimeZone.getTimeZone(target_timezone);
                    /*
                     * System.out.println(transformTimeDataFormat .parse("2013/11/24 09:53:22+00"));
                     * 
                     * DateFormat utcFormat = new SimpleDateFormat(); TimeZone utcTime =
                     * TimeZone.getTimeZone(target_timezone); utcFormat.setTimeZone(utcTime);
                     * 
                     * Date utcDate = utcFormat.parse(utcFormat .format(transformTimeDataFormat
                     * .parse("2013/11/24 09:53:22+00")));
                     * 
                     * System.out.println("Current DateTime in UTC : "+utcFormat .format(transformTimeDataFormat
                     * .parse("2013/11/24 09:53:22+00"))+" "+utcDate);
                     */
                }

            }
        }

        // get the data for each column family and its key value pairs that
        // will be stored in it when data is imported
        JSONArray families = (JSONArray) tableData.get("families");

        for (Object familyObject : families) {
            JSONObject familyNames = (JSONObject) familyObject;
            for (Object familyNameObj : familyNames.keySet()) {
                // System.out.println(familyNames.get(familyName));
                String familyNameString = familyNameObj.toString();
                JSONArray columnData = (JSONArray) familyNames.get(familyNameString);
                // System.out.println(columnData);
                ArrayList<Column> columns = new ArrayList<Column>();

                for (Object columnObj : columnData) {
                    JSONObject columnArray = (JSONObject) columnObj;
                    JSONArray columnValues = (JSONArray) columnArray.get("columns");

                    for (Object columnValueObj : columnValues) {
                        JSONObject columnValueObject = (JSONObject) columnValueObj;
                        for (Object keyObj : columnValueObject.keySet().toArray()) {
                            String key = (String) keyObj;
                            JSONObject columnValue = (JSONObject) columnValueObject.get(key);
                            System.out.println(columnValue);
                            String colName = (String) columnValue.get("name");
                            String sourceField = (String) columnValue.get("source_field");
                            String type = (String) columnValue.get("type");
                            // System.out.println("here " + colName);
                            Column columnModel = new Column(colName, sourceField, type);
                            // System.out.println("here " + columnModel.name);
                            columns.add(columnModel);
                        }
                    }

                }

                ColumnFamily columnFamily = new ColumnFamily(columns, familyNameString);
                columnFamilies.add(columnFamily);
            }
        }

    }

    public void createTableFromDataModel(Configuration conf) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tableName));

        // add column families within which the key:values will be stored
        for (ColumnFamily cf : columnFamilies) {
            HColumnDescriptor hCD = new HColumnDescriptor(cf.name.getBytes());
            table.addFamily(hCD);
        }
        // create tables
        admin.createTable(table);
        // assertThat("this string", "this string");
        admin.close();

    }

    public void createTableFromDataModelAndImportData(Configuration conf) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException, ParseException {

        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tableName));

        // add column families within which the key:values will be stored
        for (ColumnFamily cf : columnFamilies) {
            HColumnDescriptor hCD = new HColumnDescriptor(cf.name.getBytes());
            table.addFamily(hCD);
        }
        // create tables
        admin.createTable(table);
        importCSVData(admin);

    }

    private void importCSVData(HBaseAdmin admin) throws IOException, ParseException {

        BufferedReader br = new BufferedReader(new FileReader(pathToData));
        HTable table = new HTable(admin.getConfiguration(), tableName);

        Date now = new Date();
        int lineNumber = 0;
        try {
            // get header information
            String line = br.readLine();
            HashMap<String, Integer> headerIndex = getCSVHeaders(line);

            // read next line to begin reading of values

            line = br.readLine();
            while (line != null) {
                String[] splitValues = line.split(",");

                Put p = null;
                if (rowKey.hasSpatialIndex) {
                    // p = new Put(Bytes.toBytes(rowKey.));
                } else {
                    // build the composite string and populate rowkey
                    if (rowKey.isComposite) {
                        StringBuilder sb = new StringBuilder();

                        if (hasSalting) {
                            if (saltingMethod.equals("alphabet")) {
                                Integer saltNumber = randomGenerator.nextInt(25);
                                sb.append(SALT_PREFIX_ALPHABETLETTERS[saltNumber] + rowKey.delimiter);
                            }

                        }

                        for (int i = 0; i < rowKey.sourceFields.toArray().length - 1; i++) {
                            // see if the column is a constant, if so add it on...
                            if (constants.containsKey(rowKey.sourceFields.toArray()[i])) {
                                String c = constants.get(rowKey.sourceFields.toArray()[i]);
                                // if a special value is used, then add the required value to the key
                                if (specialValues.containsKey(c)) {
                                    if (c.equals(insertionDateSpecialValue)) {
                                        sb.append(new Date().getTime() + rowKey.delimiter);
                                    }
                                } else {
                                    sb.append(constants.get(rowKey.sourceFields.toArray()[i]) + rowKey.delimiter);
                                }

                            } else {
                                int stringIndex = headerIndex.get(rowKey.sourceFields.get(i));
                                sb.append(splitValues[stringIndex] + rowKey.delimiter);
                            }
                        }
                        // get the last string index and append without
                        // delimiter
                        int stringIndex = headerIndex
                                .get(rowKey.sourceFields.get(rowKey.sourceFields.toArray().length - 1));

                        sb.append(splitValues[stringIndex]);
                        // sb.append(rowKey.sourceFields
                        // .get(rowKey.sourceFields.toArray().length - 1));

                        String rowKeyString = sb.toString();
                        // System.out.println(rowKeyString);
                        p = new Put(Bytes.toBytes(rowKeyString));
                    } else {
                        // get the first source field (should only be one)
                        p = new Put(Bytes.toBytes(rowKey.sourceFields.get(0)));
                    }

                    for (ColumnFamily cf : columnFamilies) {

                        for (Column column : cf.columns) {
                            // System.out.println(column.sourceField);
                            int valPosition = headerIndex.get(column.sourceField);
                            String val = splitValues[valPosition];

                            if (column.name.equals(timeTransformationDataField)) {

                                DateFormat targetFormat = new SimpleDateFormat();
                                targetFormat.setTimeZone(targetTimeZone);

                                String targetDateString = targetFormat.format(transformTimeDataFormat.parse(val));

                                Date targetDate = targetFormat.parse(targetDateString);

                                System.out.println("Current DateTime in UTC : " + targetDate.getTime());

                                Long targetDateSeconds = targetDate.getTime();
                                p.add(Bytes.toBytes(cf.name), Bytes.toBytes(column.name),
                                        Bytes.toBytes(targetDateSeconds.toString()));
                            } else {
                                p.add(Bytes.toBytes(cf.name), Bytes.toBytes(column.name), Bytes.toBytes(val));
                            }
                        }
                    }
                }
                table.put(p);
                lineNumber++;
                line = br.readLine();
            }

        } finally {
            br.close();
        }
        Date then = new Date();
        System.out.println("added " + lineNumber + " records in total, taking "
                + ((then.getTime() - now.getTime()) / 1000.0) + " seconds");
        admin.close();
    }

    private HashMap<String, Integer> getCSVHeaders(String headerLine) {
        HashMap<String, Integer> headerIndex = new HashMap<String, Integer>();
        int u = 0;
        for (String str : headerLine.split(",")) {
            headerIndex.put(str, u);
            System.out.println(str);
            u++;
        }
        return headerIndex;
    }

    public void importCSVData(Configuration conf) throws IOException, ParseException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        importCSVData(admin);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPathToData() {
        return pathToData;
    }

    public void setPathToData(String pathToData) {
        this.pathToData = pathToData;
    }

    public boolean isCreateNew() {
        return createNew;
    }

    public void setCreateNew(boolean createNew) {
        this.createNew = createNew;
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    public void setRowKey(RowKey rowKey) {
        this.rowKey = rowKey;
    }

    public ArrayList<ColumnFamily> getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(ArrayList<ColumnFamily> columnFamilies) {
        this.columnFamilies = columnFamilies;
    }

}
