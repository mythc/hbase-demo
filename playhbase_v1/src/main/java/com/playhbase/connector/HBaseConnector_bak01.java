package com.playhbase.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class HBaseConnector_bak01
{
    static Admin admin = null;
    static Connection conn = null;
    static Configuration conf = null;
    static Table table = null;
    static String tableName = "test-hbase07";

    public HBaseConnector_bak01() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
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


    public static void main( String[] args ) throws IOException {
        System.out.println( "Hello World!" );

        HBaseConnector_bak01 hc = new HBaseConnector_bak01();

        JSONArray jsonArray = new JSONArray();

        hc.scanTables("126", "1527040783", "1527099783", jsonArray);

        System.out.println(jsonArray.toString());
    }
}
