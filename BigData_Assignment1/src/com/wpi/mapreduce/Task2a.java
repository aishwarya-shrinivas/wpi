package com.wpi.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/* Task 2a: Write a job(s) that reports all Facebook users (name, and hobby) whose Nationality is
 * the same as your own Nationality (pick one). Note that nationalities in the data file are
 * a random sequence of characters unless you work with meaningful strings like Chinese
 * or German. This is up to you.).
 * 
 * Data-set Used:
 *  1) MyPage.csv
 * 
 * Job Type:
 * 	Mapper ONLY job
 * 
 * 
 * Main Method that takes in two user defined arguments:
 * 	1) Input Path - Path of the data-set file
 * 	2) Output Path - Path where the output will be saved
 *
 * Mapper Workflow:
 * 
 * 1) Here, we would use ONLY mapper to solve this problem. 
 * 2) The mapper reads the data-set file line by line.
 * 3) We would split the data-set based on commas and store it in an array 
 * 4) We will compare if the nationality stored in the array is same as ours (Randomly generated value)
 * 5) If the nationality matches, we would output the name and hobby of the person
 * 
 * No Reducer is needed for this task as there is no aggregation/combining of values required
 */


public class Task2a {

	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2a");

		job.setJarByClass(Task2a.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<Object, Text, Void, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split(",");

			if (data[2].equals("WUKEFLUTLFNCJEULN")) {
				context.write(null, new Text(data[1] + "," + data[4]));
			}
		}
	}

}
