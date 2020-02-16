package com.wpi.mapreduce;

import java.io.IOException;
import java.util.TreeSet;

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

public class Task2f {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "wordcount");
		j.setJarByClass(Task2f.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			
				
				con.write(new Text(words[1]), new IntWritable(Integer.parseInt(words[4])));
			}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			
			TreeSet<Integer>accessDate = new TreeSet<Integer>();
			for(IntWritable value:values) {
				accessDate.add(value.get());
				}
			if(accessDate.last()<940000)
			{
				con.write(null, key);
			}
			
			
			
			
			
		}
	}
}
