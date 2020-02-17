package com.wpi.mapreduce;

import java.io.IOException;

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

/* Task 2d: For each Facebook page, compute the “happiness factor” of its owner. That is, for each
Facebook page in your data-set, report the owner’s name, and the number of people
listing him or her as friend.
 * 
 * Data-set Used:
 * 1)MyPage.csv
 * 2)AllFriends.csv
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
 *  FriendsMapper Workflow:
 *  
 *   1) Reads the data-set line by line
 *   2) Split the line based on commas and save it in an array
 *   3) The key value pair stored in context.write is:
 *  	key - MyFriend
 *  	value - PersonID (We also add in a string: allFriends to help us find out which data is coming into the reducer
 * 
 * Reducer Workflow:
 * 
 * 1) The reducer will check which mapper information is being received using the if conditions
 * 2) We use the simple counter to check for the number of friends the owner has
 */

public class Task2d {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2d");

		job.setJarByClass(Task2d.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FriendsMapper.class);
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

	public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] allFriends = line.split(",");

			con.write(new Text(allFriends[2]), new Text("allFriends:" + allFriends[1]));

		}
	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			String name = "";
			String id = "";
			int count = 0;

			for (Text value : values) {

				String[] split = value.toString().split(":");

				if (value.toString().contains("userInformation")) {
					name = split[2];
					id = split[1];

				} else if (value.toString().contains("allFriends")) {
					count++;
				}
			}

			con.write(null, new Text(id + "," + name + "," + count));
		}
	}

}
