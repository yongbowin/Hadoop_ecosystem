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
 * ��;��HBase���CRUD����
 * Administrator:yongbo.wang
 * ClassName:com.wyb.hbase.TestHBaseDDL
 * 2016��11��28�� ����9:30:58
 *
 */
public class TestHBaseDDL {
	//��Ϊÿ�ζ�Ҫ�ȴ���Configuration�������Խ�����ھ�̬����
	private static Configuration conf;
	
	static{
		conf = HBaseConfiguration.create();
	}
	
	/**
	 * �������ֿռ�
	 */
	@Test
	public void createNamespace() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		//ͨ��builder����ns2�����ռ�
		NamespaceDescriptor nsd = NamespaceDescriptor.create("ns1").build();
		admin.createNamespace(nsd);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * ������
	 */
	@Test
	public void createTable() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		//***�´�ִ�е�ʱ����Ҫ���±�������Ϊtab5�Ѿ�����
		TableName name = TableName.valueOf("ns1:tab5");
		HTableDescriptor htd = new HTableDescriptor(name);
		
		//�������base����put��ʱ��Ż������
		HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("base"));
		htd.addFamily(hcd);
		//�������addr
		hcd = new HColumnDescriptor(Bytes.toBytes("addr"));
		htd.addFamily(hcd);
		
		admin.createTable(htd);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * ����в�������
	 */
	@Test
	public void putData() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//��ȡ����������ѡ��ոմ����ı�ns1:tab4��
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//��װput����
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
		put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("age"), Bytes.toBytes(12));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("province"), Bytes.toBytes("hebei"));
		put.addColumn(Bytes.toBytes("addr"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
		
		table.put(put);
		
		System.out.println("over");
	}
	
	/**
	 * ����в������ݼ����������ݣ�
	 */
	@Test
	public void putDatas() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//��ȡ����������ѡ��ոմ����ı�ns1:tab4��
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		List<Put> puts = new ArrayList<Put>();
		for(int i=2; i<1000; i++){
			//��װput����
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
	 * ɾ��ĳһ������
	 */
	@Test
	public void delete() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//��ȡ����������ѡ��ոմ����ı�ns1:tab4��
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//ɾ�����һ��
		Delete delete = new Delete(Bytes.toBytes("row999"));
		table.delete(delete);
		
		System.out.println("over");
	}
	
	/**
	 * ��ѯһ����¼
	 */
	@Test
	public void getOne() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//��ȡ����������ѡ��ոմ����ı�ns1:tab4��
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//��ȡ�������
		Get get = new Get(Bytes.toBytes("row998"));
		Result rs = table.get(get);
		
		//�õ�ÿһ�е�����cell
		List<Cell> cells = rs.listCells();
		for(Cell cell : cells){
			//��ȡ�к�
			String rowid = Bytes.toString(cell.getRow());
			//��ȡ����
			String family = Bytes.toString(cell.getFamily());
			//��ȡ����
			String col = Bytes.toString(cell.getQualifier());
			//��ȡʱ���
			long ts = cell.getTimestamp();
			//��ȡֵ
			String value = Bytes.toString(cell.getValue());
			System.out.println(rowid + " : " + family + " : " + col + " : " + value);
		}
		
		System.out.println("over");
	}
	
	/**
	 * ��ѯ���ȫ������
	 */
	@Test
	public void findAll() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//��ȡ����������ѡ��ոմ����ı�ns1:tab4��
		TableName name = TableName.valueOf("ns1:tab5");
		Table table = conn.getTable(name);
		
		//��ѯ�ӵ�501�е���800�е����ݣ�ǰ���󲻺����ǰ����ַ����Ĵ�С���в�ѯ�ģ�
		//����scan����
		Scan scan = new Scan(Bytes.toBytes("row501"), Bytes.toBytes("row800"));
		//ɨ��ָ��������,��ֻ��ѯaddr����
		scan.addFamily(Bytes.toBytes("addr"));
		//ɨ��ָ������
		//scan.addColumn(family, qualifier);
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> it = scanner.iterator();
		while(it.hasNext()){
			Result r = it.next();
			//���ý���������
			outResult(r);
		}
		
		System.out.println("over");
	}
	
	/**
	 * �г����еı�
	 */
	@Test
	public void listTableNames() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		TableName[] names = admin.listTableNamesByNamespace("ns1");
		for(TableName n : names){
			System.out.println(n);
		}
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * ɾ����
	 */
	@Test
	public void dropTable() throws Exception {
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		//����tablename����valueOf������ȡ����
		TableName name = TableName.valueOf("ns1:tab6");
		//�Ƚ��ñ�
		admin.disableTable(name);
		//��ɾ����
		admin.deleteTable(name);
		admin.close();
		
		System.out.println("over");
	}
	
	/**
	 * ��װ����������getOne()��findAll()��������
	 */
	private void outResult(Result rs){
		//�õ�ÿһ�е�����cell
		List<Cell> cells = rs.listCells();
		for(Cell cell : cells){
			//��ȡ�к�
			String rowid = Bytes.toString(cell.getRow());
			//��ȡ����
			String family = Bytes.toString(cell.getFamily());
			//��ȡ����
			String col = Bytes.toString(cell.getQualifier());
			//��ȡʱ���
			long ts = cell.getTimestamp();
			//��ȡֵ
			String value = Bytes.toString(cell.getValue());
			System.out.println(rowid + " : " + family + " : " + col + " : " + value);
		}
	}
	
	/**
	 * �õ�������򣨷ָ
	 * @throws Exception
	 */
	@Test
	public void getTableRegions() throws Exception{
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//����tablename����valueOf������ȡ����
		TableName name = TableName.valueOf("ns1:tab5");
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		//�õ������������
		List<HRegionInfo> infos = admin.getTableRegions(name);
		for(HRegionInfo info : infos){
			//��ӡ��ns1:tab5,,1481767406833.5c9e2d34a7f04f2649b521b4274d6623.
			System.out.println(info.getRegionNameAsString());
		}
		
		//�Ա�����иִ�к�ͽ���ֳ�������
		admin.splitRegion(Bytes.toBytes("ns1:tab5,,1481784978421.05e072977b8a1101eea4038ce0d012b7."));
		
		System.out.println("over");
	}
	
	/**
	 * �ϲ��������
	 * @throws Exception
	 */
	@Test
	public void mergeTableRegions() throws Exception{
		//ͨ�������������Ӷ���
		Connection conn = ConnectionFactory.createConnection(conf);
		//����tablename����valueOf������ȡ����
		TableName name = TableName.valueOf("ns1:tab5");
		//ͨ�����ӵõ�������󣬴�����ʱ��Ҫ�õ�admin
		Admin admin = conn.getAdmin();
		
		//�õ������������
		List<HRegionInfo> infos = admin.getTableRegions(name);
		for(HRegionInfo info : infos){
			//��ӡ��ns1:tab5,,1481767406833.5c9e2d34a7f04f2649b521b4274d6623.
			System.out.println(info.getRegionNameAsString());
		}
		
		//�Ա��������кϲ�
		byte[] a = Bytes.toBytes("ns1:tab5,,1481785044741.66235fc6c845381371d8666d77d07396.");
		byte[] b = Bytes.toBytes("ns1:tab5,row744,1481785044741.1d9bfb79d16959e30f5424b44ef82e48.");
		
		admin.mergeRegions(a, b, true);
		
		System.out.println("over");
	}
	
}
