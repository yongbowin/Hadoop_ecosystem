package com.wyb.hbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * 
 * 用途：HBase表的CRUD操作
 * Administrator:yongbo.wang
 * ClassName:com.wyb.hbase.TestHBaseDDL
 * 2016年11月28日 上午9:30:58
 *
 */
public class TestHBaseDDL {
	//因为每次都要先创建Configuration对象，所以将其放在静态块中
	private static Configuration conf;
	
	static{
		conf = HBaseConfiguration.create();
	}
	
	/**
	 * 创建名字空间
	 */
	@Test
	public void createNamespace() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		//通过builder构建ns2命名空间
		NamespaceDescriptor nsd = NamespaceDescriptor.create("ns1").build();
		admin.createNamespace(nsd);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * 创建表
	 */
	@Test
	public void createTable() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		//***下次执行的时候需要改下表名，因为tab5已经存在
		TableName name = TableName.valueOf("ns1:tab5");
		HTableDescriptor htd = new HTableDescriptor(name);
		
		//添加列族base，在put的时候才会添加列
		HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("base"));
		htd.addFamily(hcd);
		//添加列族addr
		hcd = new HColumnDescriptor(Bytes.toBytes("addr"));
		htd.addFamily(hcd);
		
		admin.createTable(htd);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * 向表中插入数据
	 */
	@Test
	public void putData() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取表名（这里选择刚刚创建的表ns1:tab4）
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//组装put对象
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(12));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("province"), Bytes.toBytes("hebei"));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
		
		table.put(put);
		
		System.out.println("over");
	}
	
	/**
	 * 向表中插入数据集（多条数据）
	 */
	@Test
	public void putDatas() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取表名（这里选择刚刚创建的表ns1:tab4）
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		List<Put> puts = new ArrayList<Put>();
		for(int i=2; i<1000; i++){
			//组装put对象
			Put put = new Put(Bytes.toBytes("row" + i));
			put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
			put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(12 + i));
			put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("province"), Bytes.toBytes("hebei" + i));
			put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("city"), Bytes.toBytes("beijing" + i));
			
			puts.add(put);
		}
		
		table.put(puts);
		
		System.out.println("over");
	}
	
	/**
	 * 删除某一行数据
	 */
	@Test
	public void delete() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取表名（这里选择刚刚创建的表ns1:tab4）
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//删除最后一行
		Delete delete = new Delete(Bytes.toBytes("row999"));
		table.delete(delete);
		
		System.out.println("over");
	}
	
	/**
	 * 查询一条记录
	 */
	@Test
	public void getOne() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取表名（这里选择刚刚创建的表ns1:tab4）
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//获取结果对象
		Get get = new Get(Bytes.toBytes("row998"));
		Result rs = table.get(get);
		
		//得到每一行的所有cell
		List<Cell> cells = rs.listCells();
		for(Cell cell : cells){
			//获取行号
			String rowid = Bytes.toString(cell.getRow());
			//获取列族
			String family = Bytes.toString(cell.getFamily());
			//获取列名
			String col = Bytes.toString(cell.getQualifier());
			//获取时间戳
			long ts = cell.getTimestamp();
			//获取值
			String value = Bytes.toString(cell.getValue());
			System.out.println(rowid + " : " + family + " : " + col + " : " + value);
		}
		
		System.out.println("over");
	}
	
	/**
	 * 查询表的全部数据
	 */
	@Test
	public void findAll() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//获取表名（这里选择刚刚创建的表ns1:tab4）
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//查询从第501行到第800行的数据（前含后不含，是按照字符串的大小进行查询的）
		//创建scan对象
		Scan scan = new Scan(Bytes.toBytes("row501"), Bytes.toBytes("row800"));
		//扫描指定的列族,即只查询addr列族
		scan.addFamily(Bytes.toBytes("addr"));
		//扫描指定的列
		//scan.addColumn(family, qualifier);
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result r = it.next();
			//调用结果输出函数
			outResult(r);
		}
		
		System.out.println("over");
	}
	
	/**
	 * 列出所有的表
	 */
	@Test
	public void listTableNames() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		TableName[] names = admin.listTableNamesByNamespace("ns1");
		for(TableName n : names){
			System.out.println(n);
		}
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * 删除表
	 */
	@Test
	public void dropTable() throws Exception {
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		//创建tablename对象，valueOf方法获取表名
		TableName name = TableName.valueOf("ns1:tab6");
		//先禁用表
		admin.disableTable(name);
		//再删除表
		admin.deleteTable(name);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * 封装输出结果，供getOne()和findAll()函数调用
	 */
	private void outResult(Result rs){
		//得到每一行的所有cell
		List<Cell> cells = rs.listCells();
		for(Cell cell : cells){
			//获取行号
			String rowid = Bytes.toString(cell.getRow());
			//获取列族
			String family = Bytes.toString(cell.getFamily());
			//获取列名
			String col = Bytes.toString(cell.getQualifier());
			//获取时间戳
			long ts = cell.getTimestamp();
			//获取值
			String value = Bytes.toString(cell.getValue());
			System.out.println(rowid + " : " + family + " : " + col + " : " + value);
		}
	}
	
	/**
	 * 得到表的区域（分割）
	 * @throws Exception
	 */
	@Test
	public void getTableRegions() throws Exception{
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//创建tablename对象，valueOf方法获取表名
		TableName name = TableName.valueOf("ns1:tab5");
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		//得到表的所有区域
		List<HRegionInfo> infos = admin.getTableRegions(name);
		for(HRegionInfo info : infos){
			//打印出ns1:tab5,,1481767406833.5c9e2d34a7f04f2649b521b4274d6623.
			System.out.println(info.getRegionNameAsString());
		}
		
		//对表进行切割，执行后就将表分成两部分
		admin.splitRegion(Bytes.toBytes("ns1:tab5,,1481784978421.05e072977b8a1101eea4038ce0d012b7."));
		
		System.out.println("over");
	}
	
	/**
	 * 合并表的区域
	 * @throws Exception
	 */
	@Test
	public void mergeTableRegions() throws Exception{
		//通过工厂创建连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		//创建tablename对象，valueOf方法获取表名
		TableName name = TableName.valueOf("ns1:tab5");
		//通过连接得到管理对象，创建表时需要用到admin
		Admin admin = conn.getAdmin();
		
		//得到表的所有区域
		List<HRegionInfo> infos = admin.getTableRegions(name);
		for(HRegionInfo info : infos){
			//打印出ns1:tab5,,1481767406833.5c9e2d34a7f04f2649b521b4274d6623.
			System.out.println(info.getRegionNameAsString());
		}
		
		//对表的区域进行合并
		byte[] a = Bytes.toBytes("ns1:tab5,,1481785044741.66235fc6c845381371d8666d77d07396.");
		byte[] b = Bytes.toBytes("ns1:tab5,row744,1481785044741.1d9bfb79d16959e30f5424b44ef82e48.");
		
		admin.mergeRegions(a, b, true);
		
		System.out.println("over");
	}
	
}
