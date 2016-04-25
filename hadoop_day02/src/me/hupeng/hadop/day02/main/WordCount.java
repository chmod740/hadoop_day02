package me.hupeng.hadop.day02.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception{
		
		
		Configuration configuration = new Configuration();
		
		
		//使用了单例的方式构造了一个实例
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://127.0.0.1:9000/wordcount/data/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/wordcount/out2/"));
		
		System.exit(job.waitForCompletion(true)?0:1);
		 
	}
}
