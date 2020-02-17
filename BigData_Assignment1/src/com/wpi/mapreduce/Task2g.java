package com.wpi.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
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

/* Task 2g: Identify people that have declared someone as their friend yet who have never accessed
their respective friend’s Facebook page – indicating that they don’t care enough to find
out any news about their friend (at least not via Facebook).
 * 
 * Data-set Used:
 * 1)AllFriends.csv
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
 * AllFriendsMapper Workflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - PersonID
 *  	value - MyFriend (We also add in a string: AllFriends to help us find out which data is coming into the reducer
 *  
 *  AccessLogMapper Workflow:
 *  
 *   1) Reads the data-set line by line
 *   2) Split the line based on commas and save it in an array
 *   3) The key value pair stored in context.write is:
 *  	key - byWho
 *  	value - WhatPage (We also add in a string: accessLog to help us find out which data is coming into the reducer
 *  
 *  
 * Reducer Workflow:
 * 
 *  1) We use an ArrayList to save all the friends of the person. This is populated using the AllFriendsMapper
 *  2) We use a HashMap with key as their friend and value as they access it (1)
 *  3) We then loop through the ArryList and check if the friend id is present in the hasmap
 *  4) If the friendID is not present in the hashmap then the person has not accessed their friends facebook page
 */

public class Task2g {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2g");

		job.setJarByClass(Task2g.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AllFriendsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AccessLogMapper.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class AllFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] friendshipStrength = line.split(",");

			con.write(new Text(friendshipStrength[1]), new Text("AllFriends:" + friendshipStrength[2]));
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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			HashMap<String, Integer> friendsAccess = new HashMap<String, Integer>();
			ArrayList<String> friends = new ArrayList<String>();

			for (Text value : values) {

				String[] split = value.toString().split(":");

				if (value.toString().contains("AllFriends")) {
					friends.add(split[1]);
				}

				else if (value.toString().contains("accessLog")) {
					if (friendsAccess.containsKey(split[1])) {
						friendsAccess.put(split[1], 1);
					}
				}
			}

			for (String id : friends) {

				if (!friendsAccess.containsKey(id)) {
					con.write(null, new Text(key + "," + id));
				}

			}

		}
	}
}