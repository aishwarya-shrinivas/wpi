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

public class Task2d {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 2d");

        job.setJarByClass(Task2d.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, userMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, friendsMapper.class);
        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	public static class userMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userDetails = line.split(",");
			
				con.write(new Text(userDetails[0]), new Text("userInformation:"+userDetails[0]+":"+userDetails[1]));
			}
	}

	public static class friendsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] allFriends = line.split(",");
			
			con.write(new Text(allFriends[2]), new Text("allFriends:"+allFriends[1]));
			
			
			}
	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {
			
			
			String name = "";
			String id = "";
			int count = 0;
			
			for(Text value : values) {
				
				String [] split = value.toString().split(":");
				
				if(value.toString().contains("userInformation")) {
					name = split[2];
					id= split[1];
					
				}
				else if(value.toString().contains("allFriends")) {
					count++;
				}
			}
			
			con.write(null,new Text(id+","+name+","+count));
		}
}
	
}
