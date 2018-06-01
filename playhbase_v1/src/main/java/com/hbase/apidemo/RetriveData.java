package com.hbase.apidemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class RetriveData {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("demo"));

        Get g = new Get(Bytes.toBytes("row1"));

        Result result = table.get(g);
        System.out.println(result);

        byte[] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));
        byte[] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));

        String name = Bytes.toString(value);
        String city = Bytes.toString(value1);

        System.out.println("name: " + name + " city: " + city);
    }
}
