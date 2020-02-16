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

public class Task2g {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 2g");

        job.setJarByClass(Task2g.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, allFriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, accessLogMapper.class);
        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	public static class allFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] friendshipStrength = line.split(",");
			
				con.write(new Text(friendshipStrength[1]), new Text("AllFriends:"+friendshipStrength[2]));
			}
	}

	public static class accessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] accessLogDetails = line.split(",");
			Text byWho = new Text(accessLogDetails[1]);
			Text whatPage = new Text("accessLog:"+accessLogDetails[2]);
	
				con.write(byWho,whatPage);
			}
	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context con)
				throws IOException, InterruptedException {
			
			
			HashMap<String,Integer>friendsAccess = new HashMap<String,Integer>();
			ArrayList<String>friends = new ArrayList<String>();
			
			for(Text value: values ) {
				
				String [] split = value.toString().split(":");
				
				if(value.toString().contains("AllFriends")) {
					friends.add(split[1]);
				}
				
				else if(value.toString().contains("accessLog")) {
					if(friendsAccess.containsKey(split[1])) {
						friendsAccess.put(split[1], 1);	
					}
				}
			}
			
			for(String id : friends) {
				
				if(! friendsAccess.containsKey(id)) {
					con.write(null, new Text(key+","+id));
				}
				
			}
			
			
		}
	}
}