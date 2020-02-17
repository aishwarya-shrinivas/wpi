package com.wpi.mapreduce;

import java.io.IOException;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* Task 2e: Determine which people have favorites. That is, for each Facebook page owner,
determine how many total accesses to Facebook pages they have made (as reported in
the AccessLog) and how many distinct Facebook pages they have accessed in total.
 * 
 * Data-set Used:
 * 1)MyPage.csv
 * 2)AccessLog.csv
 * 
 * 
 * Job Type:
 * 	2 Mappers and a Reducer Job
 * 
 * 
 * Main Method that takes in three user defined arguments:
 * 	1) We use MultipleInputs to provide the location of the two datasets and associated mappers to be used
 * 	2) Output Path - Path where the output will be saved
 * 
 * UserMapper Workflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - PersonID
 *  	value - PersonID and Owners name (We also add in a string: userInformation to help us find out which data is coming into the reducer
 *  4) This data helps in mapping the the owner name to the number of friends using the person ID key
 *  
 * AccessLogMapper Workflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - byWho
 *  	value - WhatPage (We also add in a string: userInformation to help us find out which data is coming into the reducer
 *  4) This data helps in mapping the the owner name to the number of friends using the person ID key
 *  
 *  
 * Reducer Workflow:
 * 
 * 1) The reducer will check which mapper information is being received using the if conditions
 * 2) We use the simple counter to check total accesses to Facebook pages
 * 3) We also push the data into a hashmap and then use hashmap.size() to get distinct facebook page access
 */

public class Task2e {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2e");

		job.setJarByClass(Task2e.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogMapper.class);
		job.setReducerClass(reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userDetails = line.split(",");

			con.write(new Text(userDetails[0]), new Text("userInformation:" + userDetails[0] + ":" + userDetails[1]));
		}
	}

	public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] accessLogDetails = line.split(",");
			Text byWho = new Text(accessLogDetails[1]);
			Text whatPage = new Text("accessLog:" + accessLogDetails[2]);

			con.write(byWho, whatPage);
		}
	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			HashMap<String, Integer> map = new HashMap<String, Integer>();
			String name = "";
			String id = "";
			int count = 0;

			for (Text value : values) {

				String[] split = value.toString().split(":");

				if (value.toString().contains("userInformation")) {
					name = split[2];
					id = split[1];

				} else if (value.toString().contains("accessLog")) {
					count++;
					if (!map.containsKey(split[1])) {
						map.put(split[1], 1);
					}

				}
			}

			con.write(null, new Text(id + "," + name + "," + count + "," + map.size()));
		}
	}
}
