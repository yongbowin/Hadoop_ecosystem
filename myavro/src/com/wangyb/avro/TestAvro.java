package com.wangyb.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * 测试avro串行化
 */
public class TestAvro {
	public static void main(String[] args) throws Exception {
		//创建解析器对象
		Parser parser = new Parser();
		
		//这种方式取得是绝对路径，应该把employee.avsc文件和类TestAvro放在一个路径下
		//加载和指定的类在一起的那个文件,其中的属性值应该小写
		InputStream in = TestAvro.class.getResourceAsStream("employee.avsc");
		
		//开始解析avsc文件
		Schema schema = parser.parse(in);
		
		//创建GenericRecord对象，并设置相应的值
		GenericRecord record = new GenericData.Record(schema);
		record.put("name", "tom");
		record.put("age", 12);
		
		//内存数据
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		//创建写入器对象
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
		
		//写入record数据到流中
		writer.write(record, encoder);
		encoder.flush();
		baos.close();
		
		System.out.println("over");
	}
}
