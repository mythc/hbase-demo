package com.hbase.apidemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("demo"));

        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("professional"));

        ResultScanner scanner = table.getScanner(scan);

        for(Result result=scanner.next(); result!=null;result=scanner.next())
            System.out.println(result);
        scanner.close();
    }
}
