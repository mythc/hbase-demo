package com.hbase.apidemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class DeleteTable {
    public static void main(String[] args) throws IOException {
        //Instantiating configuration class
        Configuration con = HBaseConfiguration.create();
        con.set("hbase.zookeeper.quorum", "10.18.218.17:2181,10.18.218.5:2181,10.18.218.9:2181");


        //Instantiating HbaseAdmin class
        Connection conn = ConnectionFactory.createConnection(con);
        Admin admin = conn.getAdmin();

        HColumnDescriptor columnDescriptor = new HColumnDescriptor("colTest");

        admin.disableTable(TableName.valueOf("demo"));
        admin.deleteTable(TableName.valueOf("demo"));

        System.out.println("Table deleted");
    }
}
