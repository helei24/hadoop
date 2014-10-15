package test;

import hbase.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HBaseTest {

    // 测试创建表
    @Test
    public void testCreateTable() throws IOException {

        HBaseUtil.createTable("user", "username", "password");
    }

    // 测试插入数据
    @Test
    public void testInsertData() throws IOException {

        HTable htable = HBaseUtil.getHTable("user");
        List<Put> puts = new ArrayList<Put>();
        ;
        Put put1 = HBaseUtil.getPut("1", "username", null, "chuliuxiang");
        puts.add(put1);
        Put put2 = HBaseUtil.getPut("1", "password", null, "654321");
        puts.add(put2);
        htable.setAutoFlushTo(false);
        htable.put(puts);
        htable.flushCommits();
        htable.close();
    }

    // 测试获取一行数据
    @Test
    public void testGetRow() throws IOException {

        Result result = HBaseUtil.getResult("user", "1");
        byte[] userName = result.getValue("username".getBytes(), null);
        byte[] password = result.getValue("password".getBytes(), null);
        System.out.println("username is: " + Bytes.toString(userName) + " passwd is :"
                + Bytes.toString(password));
    }

    // 测试条件查询
    @Test
    public void testScan() throws IOException {

        ResultScanner rs = HBaseUtil.getResultScanner("user", "username", "chuliuxiang", "1", "10");
        for (Result r : rs) {
            System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("username"), null)));
        }
    }
}
