package com.wpi.mapreduce;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/* Task 2h: Report all owners of a Facebook who are famous and happy, namely, those who have
more friends than the average number of friends across all owners in the data files.
 * 
 * Data-set Used:
 * 1)AllFriends.csv
 * 
 * 
 * 
 * Job Type:
 * 	1 Mapper, 1 Reducer
 * 
 * 
 * Main Method that takes in two user defined arguments:
 * 	1) Input Path - Path of the data-set file (Takes 2 inputs from two data-sets)
 * 	2) Output Path - Path where the output will be saved
 * 
 * Mapper1 Workflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - PersonID
 *  	value - simple IntWritable(1)
 *  
 *  
 * Reducer Workflow:
 * 
 *   1) We create a hashmap where we store the personID as the key and count of number of friends he/she has into it's value
 *   2) WE also use a total variable that counts the total number of friends all the personID's have 
 *   3) We then use the cleanup method to calculate the average based on the total/number of persons
 *   4) We loop through the hashmap and check if the personID has friends more than the average or not
 *   4) If the personID has friends more than the average then we print the results 
 */

public class Task2h {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(",");

			word.set(data[1]);
			context.write(word, one);
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		private HashMap<String, Integer> map = new HashMap<String, Integer>();
		private double total = 0.0;

		public void reduce(Text key, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			total += sum;
			if (!map.containsKey(key.toString())) {
				map.put(key.toString(), sum);
			}
		}

		public void cleanup(Context con) throws IOException, InterruptedException {

			double average = total / map.size();

			for (String personId : map.keySet()) {

				if (map.get(personId) > average) {
					con.write(new Text(personId), new IntWritable(map.get(personId)));
				}
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Task 2h");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
