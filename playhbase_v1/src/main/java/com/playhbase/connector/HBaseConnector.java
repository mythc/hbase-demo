package com.playhbase.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Hello world!
 */
public class HBaseConnector {
    static Admin admin = null;
    static Connection conn = null;
    static Configuration conf = null;
    static Table table = null;
    static String tableName = "test-hbase07";

    public HBaseConnector() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        table = conn.getTable(TableName.valueOf(tableName));
    }

    public void scanTables(String devId, String startTime, String stopTime, List<Map<String, Object>> list) throws IOException {

        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(devId + "_" + startTime));
        scan.setStopRow(Bytes.toBytes(devId + "_" + stopTime));


        ResultScanner scanner = table.getScanner(scan);

        for (Result r : scanner) {
            for (Cell cell : r.rawCells()) {
                Map<String, Object> map = new HashMap<String, Object>();
                String[] str = Bytes.toString(r.getRow()).split("[_.]");
                map.put("devId", str[0]);
                map.put("timestamp", str[1]);
                map.put("orderStatus", str[2]);
                map.put("value", Bytes.toDouble(CellUtil.cloneValue(cell)));
                list.add(map);
            }
        }
    }


    public double scanFirstData(String devId, String startTime, String stopTime) throws IOException {

        double result = 0;

        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setCaching(1);
        scan.setStartRow(Bytes.toBytes(devId + "_" + startTime));


        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();

        for (Cell cell : r.rawCells()) {

            result = Bytes.toDouble(CellUtil.cloneValue(cell));

        }

        return result;
    }

    public double scanLastData(String devId, String startTime, String stopTime) throws IOException {

        double result = 0;

        table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setReversed(true);
        scan.setCaching(1);
        scan.setStartRow(Bytes.toBytes(devId + "_" + stopTime));

        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();

        for (Cell cell : r.rawCells()) {

            result = Bytes.toDouble(CellUtil.cloneValue(cell));

        }

        return result;
    }

    public void getRanking(String[] devIds, String startTime, String stopTime, List<Map<String, Object>> list) {

        for (String devid : devIds) {
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                map.put("devId", devid.replaceAll("\"", ""));
                map.put("value", scanLastData(devid.replaceAll("\"", ""), startTime.replaceAll("\"", ""), stopTime.replaceAll("\"", "")) - scanFirstData(devid.replaceAll("\"", ""), startTime.replaceAll("\"", ""), stopTime.replaceAll("\"", "")));
            } catch (Throwable throwable) { //IOException e 捕获不到所有异常
                map.put("devId", devid.replaceAll("\"", ""));
                map.put("value", 0.0); //所给的时间范围内没有该设备的数据
            }
            list.add(map);
        }

        Collections.sort(list, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> o1, Map<String, Object> o2) {

                double v1 = (Double)o1.get("value");
                double v2 = (Double)o2.get("value");

                return (int)(v1 - v2);
            }
        });

    }


    public static void main(String[] args) {

        HBaseConnector hc = null;
        try {
            hc = new HBaseConnector();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String[] devIds = {"\"122\"", "123", "124", "125", "126", "128"};
//        String[] devIds = {"128"};

        String[] timestamp = {"1527068560736", "1527073735865", "1527078600857", "1527083797972", "1527088656839", "1527093414609", "1527098279450", "1527103153345", "1527108084384", "1527113032433", "1527118119344", "1527123121310"};

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        hc.getRanking(devIds, "1527068560736", "1527073735865", list);

        System.out.println(list);

    }
}
