package com.wpi.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2a {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task 2A");

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
