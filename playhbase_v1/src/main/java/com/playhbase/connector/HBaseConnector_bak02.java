package com.playhbase.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Hello world!
 *
 */
public class HBaseConnector_bak02
{
    static Admin admin = null;
    static Connection conn = null;
    static Configuration conf = null;
    static Table table = null;
    static String tableName = "test-hbase07";

    public HBaseConnector_bak02() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        table = conn.getTable(TableName.valueOf(tableName));
    }

    public void listTables() throws IOException {
        // Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] tableDescriptor =admin.listTables();

        // printing all the table names.
        for (int i=0; i<tableDescriptor.length;i++ ){
            System.out.println(tableDescriptor[i].getNameAsString());
        }
    }

    public void scanTables(String devId, String startTime, String stopTime, JSONArray jsonArray) throws IOException {

        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(devId + "_" + startTime));
        scan.setStopRow(Bytes.toBytes(devId + "_" + stopTime));


//        List<Filter> filters = new ArrayList<Filter>();
//        Filter filter2 = new PageFilter(10);
//        filters.add(filter2);
//
//        FilterList fl = new FilterList(filters);
//
//        scan.setFilter(fl);

        ResultScanner scanner = table.getScanner(scan);

//        SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        for(Result r : scanner) {
            for (Cell cell : r.rawCells()) {
                Map<String, Object> map = new HashMap<String, Object>();
                String[] str = Bytes.toString(r.getRow()).split("[_.]");
                map.put("devId", str[0]);
//                String date = format.format(new Long(Long.parseLong(str[1])));
                map.put("timestamp", str[1]);
                map.put("orderStatus", str[2]);
                map.put("value", Bytes.toDouble(CellUtil.cloneValue(cell)));
                jsonArray.put(map);
            }
        }
    }

    public void scanLastData(String devId, String startTime, String stopTime, JSONArray jsonArray) throws IOException {

//        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
//        scan.setLimit();
        scan.setReversed(true);
        scan.setStartRow(Bytes.toBytes(devId + "_" + stopTime));

//        scan.setStartRow(Bytes.toBytes(devId + "_" + startTime));
//        scan.setStopRow(Bytes.toBytes(devId + "_" + stopTime));


//        List<Filter> filters = new ArrayList<Filter>();
//        Filter filter2 = new PageFilter(1000);
//        filters.add(filter2);
//
//        FilterList fl = new FilterList(filters);
//
//        scan.setFilter(fl);

//        scan.setBatch(1);
        scan.setCaching(1);

        ResultScanner scanner = table.getScanner(scan);

//        for(Result r : scanner) {
        Result r = scanner.next();
            for (Cell cell : r.rawCells()) {
                Map<String, Object> map = new HashMap<String, Object>();
                String[] str = Bytes.toString(r.getRow()).split("[_.]");
                map.put("devId", str[0]);
//                String date = format.format(new Long(Long.parseLong(str[1])));
                map.put("timestamp", str[1]);
                map.put("orderStatus", str[2]);
                map.put("value", Bytes.toDouble(CellUtil.cloneValue(cell)));
                jsonArray.put(map);
            }
//        }
    }

    public void scanFirstData(String devId, String startTime, String stopTime, JSONArray jsonArray) throws IOException {

//        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(devId + "_" + startTime));


        List<Filter> filters = new ArrayList<Filter>();
        Filter filter2 = new PageFilter(1);
        filters.add(filter2);

        FilterList fl = new FilterList(filters);

        scan.setFilter(fl);

        ResultScanner scanner = table.getScanner(scan);

        for(Result r : scanner) {
            for (Cell cell : r.rawCells()) {
                Map<String, Object> map = new HashMap<String, Object>();
                String[] str = Bytes.toString(r.getRow()).split("[_.]");
                map.put("devId", str[0]);
//                String date = format.format(new Long(Long.parseLong(str[1])));
                map.put("timestamp", str[1]);
                map.put("orderStatus", str[2]);
                map.put("value", Bytes.toDouble(CellUtil.cloneValue(cell)));
                jsonArray.put(map);
            }
        }
    }

    public static void main( String[] args ) throws IOException {
        System.out.println( "Hello World!" );

        HBaseConnector_bak02 hc = new HBaseConnector_bak02();

        JSONArray jsonArray = new JSONArray();

        for (int i = 0 ; i<10; i++) {
//            System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
//            hc.scanFirstData("126", "0", "9", jsonArray);
            System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
            hc.scanLastData("126", "0", "" + i, jsonArray);
            System.out.println(jsonArray.toString());
            System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));

            System.out.println("------------");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


//        System.out.println(jsonArray.toString());
    }
}
