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

/* Task 2f: Find the list of all people that have set up a Facebook page, but have lost interest, i.e.,
after some initial time unit (say 10 days or whatever you choose) have never accessed
Facebook again (meaning no entries in the Facebook AccessLog exist after that date).
 * 
 * Data-set Used: 
 * 1)AccessLog.csv
 * 
 * 
 * Job Type:
 * 	1 Mapper and a Reducer Job
 * 
 * 
 * Main Method that takes in two user defined arguments:
 * 	1) Input Path - Path of the data-set file 
 * 	2) Output Path - Path where the output will be saved
 * 
 * Mapper Workflow:
 * 
 *  1) Reads the data-set line by line
 *  2) Split the line based on commas and save it in an array
 *  3) The key value pair stored in context.write is:
 *  	key - ByWho
 *  	value - AccessTime
 *   
 * 
 * Reducer Workflow:
 * 
 * 1) In reducer, we would get the list of access time for every person
 * 2) We save the access time into a treeset. Treeset sort the data by default
 * 3) Once we insert all of the access time values into the treeset, we simply get the last value for the treeset
 * 4) If the last value is greater than a specified access time, then the person is still active else the person lost interest
 * 
 */

public class Task2f {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "Task2f");
		j.setJarByClass(Task2f.class);
		j.setMapperClass(Map.class);
		j.setReducerClass(Reduce.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");

			con.write(new Text(words[1]), new IntWritable(Integer.parseInt(words[4])));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {

			TreeSet<Integer> accessDate = new TreeSet<Integer>();
			for (IntWritable value : values) {
				accessDate.add(value.get());
			}
			if (accessDate.last() < 940000) {
				con.write(null, key);
			}

		}
	}
}
