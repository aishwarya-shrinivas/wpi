package com.wpi.mapreduce;

import java.io.IOException;
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


/* Task 2b: Write job(s) that reports for each country, how many of its citizens have a Facebook page.
 * 
 * Data-set Used:
 *  1) MyPage.csv
 *  
 * Job Type:
 * 	1 Mapper and 1 Reducer
 * 
 * Main Method that takes in two user defined arguments:
 * 	1) Input Path - Path of the data-set file
 * 	2) Output Path - Path where the output will be saved
 * 
 * Mapper Worflow:
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - Country
 *  	value - IntWritable 1
 * 
 * 
 * Reducer Workflow
 *  1) The reducer will aggregate all the values of a particular country
 *  2) This will give us the total number of citizens having a facebook page for a particular country
 * 
 */


public class Task2b {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "Task 2b");
		j.setJarByClass(Task2b.class);
		j.setMapperClass(Map.class);
		j.setReducerClass(Reduce.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split(",");
			
				Text outputKey = new Text(data[3]);
				IntWritable outputValue = new IntWritable(1);
				context.write(outputKey, outputValue);
			}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
