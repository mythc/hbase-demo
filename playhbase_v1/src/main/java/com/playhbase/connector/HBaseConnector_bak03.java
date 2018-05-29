package com.playhbase.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;

import java.io.IOException;
import java.util.*;

/**
 * Hello world!
 */
public class HBaseConnector_bak03 {

    static Admin admin = null;
    static Connection conn = null;
    static Configuration conf = null;
    static Table table = null;
    static String tableName = "test-hbase07";

    public HBaseConnector_bak03() throws IOException {
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

    public void scanTables(String devId, String startTime, String stopTime, JSONArray jsonArray) throws IOException {

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
                jsonArray.put(map);
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

    public void getRanking(String[] devIds, String startTime, String stopTime, JSONArray jsonArray) throws IOException {

        List<Map<String, Double>> list = new ArrayList<Map<String, Double>>();

        for (String devid : devIds) {
            Map<String, Double> map = new HashMap<String, Double>();
//            System.out.println(scanLastData(devid, startTime, stopTime));
//            System.out.println(scanFirstData(devid, startTime, stopTime));
//            System.out.println("----------");
            map.put(devid, scanLastData(devid, startTime, stopTime) - scanFirstData(devid, startTime, stopTime));
            list.add(map);
        }

        Collections.sort(list, new Comparator<Map<String, Double>>() {
            @Override
            public int compare(Map<String, Double> o1, Map<String, Double> o2) {

                double v1 = 0;
                double v2 = 0;

                for(String key : o1.keySet()) {
                    v1 = o1.get(key);
                }

                for (String key : o2.keySet()) {
                    v2 = o2.get(key);
                }

                return (int)(v1 - v2);
            }
        });

        for (Map<String, Double> map : list) {
            jsonArray.put(map);
        }
    }


    public static void main(String[] args) throws IOException {

        HBaseConnector_bak03 hc = new HBaseConnector_bak03();

        String[] devIds = {"122", "123", "124", "125", "126"};
        String[] timestamp = {"1527068560736", "1527073735865", "1527078600857", "1527083797972", "1527088656839", "1527093414609", "1527098279450", "1527103153345", "1527108084384", "1527113032433", "1527118119344", "1527123121310"};

        JSONArray jsonArray = new JSONArray();

        hc.getRanking(devIds, "1527068560736", "1527093414609", jsonArray);
        System.out.println(jsonArray);




//        for (int i = 0; i < timestamp.length; i++) {
//            System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
//            System.out.println(hc.scanLastData("126", "0", timestamp[i]));
//            System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
//
//            System.out.println("------------");
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }


//        System.out.println(jsonArray.toString());
    }
}
