package com.demo;

import com.demo.client.HbaseClient;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HbaseTest {


	@Test
	public void testHbase() throws IOException {
		String data = HbaseClient.getData("user", "1", "color", "red");
		System.out.println(data);
	}

	@Test
	public void testAllKey() throws IOException {
		Scan scan = new Scan();
		Table table = HbaseClient.conn.getTable(TableName.valueOf("p_history"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result r : scanner) {
			System.out.println(new String(r.getRow()));
		}

	}

	@Test
	public void testGetRow() throws IOException {
		List<Map.Entry> p_history = HbaseClient.getRow("p_history", "1");
		for (Map.Entry entry : p_history) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
		}

	}
}
