package com.wyb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * TODO:HBase的客户端程序，用来和HBase集群进行交互
 * @author yongbo.wang
 * ClassName:com.wyb.hbase.App
 * 2016年11月27日 下午5:13:35
 *
 */
public class App {
	/**
	 * 实现put 'ns1:customers1','row2','base:name','tom'
	 * @param args
	 * @throws Exception
	 */
//	@SuppressWarnings({ "resource", "deprecation" })
	public static void main(String[] args) throws Exception {
		//创建HBase的配置对象
		Configuration conf = HBaseConfiguration.create();
		//创建表
		HTable table = new HTable(conf, "ns1:customers1");
		//创建行id，是一个字节数组
		byte[] rowid = Bytes.toBytes("row2");
		//创建put对象
		Put put = new Put(rowid);
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("tomas"));
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(12));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("province"), Bytes.toBytes("hebei"));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("city"), Bytes.toBytes("shijiazhuang"));
		//进行插入
		table.put(put);
		
		System.out.println("over");
	}
}
