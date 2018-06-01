package com.hbase.apidemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class InsertData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("demo"));



        Put p = new Put(Bytes.toBytes("row2"));

//        p.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("rbju"));

//        p.addColumn(Bytes.toBytes("personal"),Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));
//        p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"),Bytes.toBytes("manager"));
        p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("salary"),Bytes.toBytes("500"));

        table.put(p);
        System.out.println("data inserted");

        table.close();
    }
}
