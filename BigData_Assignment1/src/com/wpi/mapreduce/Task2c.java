package com.wpi.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

/* Task 2c: Find the top 10 interesting Facebook pages, namely, those that got the most accesses
 * based on your AccessLog dataset compared to all other pages.
 * 
 * Data-set Used:
 * AccessLog.csv
 * 
 * Job Type:
 * 	1 Mapper and 1 Reducer
 * 
 * Main Method that takes in two user defined arguments:
 * 	1) Input Path - Path of the data-set file
 * 	2) Output Path - Path where the output will be saved
 * 
 * Mapper Worflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - WhatPage
 *  	value - IntWritable 1
 * 
 * Reducer Workflow:
 * 
 * 1) The reducer will aggregate how many times "WhatPage" has been accessed
 * 2) The details will be saved into a HashMap with key as WhatPage and value as total number of accesses made
 * 3) We use the cleanup method inside the Reducer to print out the data
 * 4) We sort the hashmap based on the values in descending order and print out the top 10 key,value pairs
 */

public class Task2c {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "Task 2c");
		j.setJarByClass(Task2c.class);
		j.setMapperClass(TaskMap.class);
		j.setReducerClass(Reduce.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class TaskMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");

			con.write(new Text(words[2]), new IntWritable(1));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		private HashMap<String, Integer> map = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			map.put(key.toString(), sum);
		}

		public void cleanup(Context con) throws IOException, InterruptedException {

			int count = 1;

			final Map<String, Integer> sortedByCount = sortByValue(map);

			for (String person : sortedByCount.keySet()) {
				con.write(new Text(person), new IntWritable(sortedByCount.get(person)));

				if (count == 10) {
					break;
				}
				count++;
			}

		}

	}

	public static Map<String, Integer> sortByValue(final Map<String, Integer> wordCounts) {
		return wordCounts.entrySet().stream().sorted((Map.Entry.<String, Integer>comparingByValue().reversed()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	}
}
