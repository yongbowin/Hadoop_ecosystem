package com.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DemoMapper {
	// Mapper Class,mapresuce的包，不是mpred的包
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable mapOutputValue = new IntWritable(1);
		private Text mapOutputKey = new Text();

		// override/implement methods
		/**
		 * 首先分割输入value为单词，组成key/value对进行输出，这里输出的value统一定义为常量1
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			//split
			StringTokenizer tokenizer = new StringTokenizer(lineValue);
			//iterator
			while(tokenizer.hasMoreTokens()){
				String wordValue = tokenizer.nextToken();
				mapOutputKey.set(wordValue);
				context.write(mapOutputKey, mapOutputValue);
			}
		}

	}

	// Reducer Class
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}

	}

	// Driver
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, DemoMapper.class.getSimpleName());
		//set
		job.setJarByClass(DemoMapper.class);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//submit
		boolean isSuccess = job.waitForCompletion(true);
		
		System.exit(isSuccess ? 0 : 1);
		
	}
}
