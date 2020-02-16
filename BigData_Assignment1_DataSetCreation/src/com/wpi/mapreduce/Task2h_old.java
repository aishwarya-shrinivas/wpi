package com.wpi.mapreduce;

import java.io.IOException;
import java.util.HashMap;

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

public class Task2h_old {

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "wordcount");
		j.setJarByClass(Task2h_old.class);
		j.setMapperClass(MapForWordCount.class);
		//j.setCombinerClass(combiner.class);
		j.setReducerClass(taskReducer.class);
		
      //  j.setNumReduceTasks(1);

		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			
				
				con.write(new Text(words[1]), new IntWritable(1));
			}
	}

	/*
	 * public static class combiner extends Reducer<Text, Text, Text, IntWritable> {
	 * 
	 * private IntWritable total = new IntWritable();
	 * 
	 * public void reduce(Text key, Iterable<IntWritable> values, Context con)
	 * throws IOException, InterruptedException {
	 * 
	 * 
	 * 
	 * int sum = 0; for (IntWritable val : values) { sum += val.get(); }
	 * total.set(sum); con.write(key, total);
	 * 
	 * 
	 * 
	 * 
	 * 
	 * }
	 * 
	 * }
	 */
		
		
	
		public static class taskReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			
		
			private HashMap<String,Integer>map = new HashMap<String, Integer>();
			private double total = 0.0;
		
			public void reduce(Text key, Iterable<IntWritable> values, Context con)
					throws IOException, InterruptedException {
				
				int sum = 0;
				for (IntWritable val : values) {
	                sum += val.get();
	            }
				
				total += sum;
				if(!map.containsKey(key.toString())) {
					map.put(key.toString(),sum);
				}
			
				con.write(key, new IntWritable(sum));
				
			}
			
			
		/*
		 * public void cleanup(Context con) throws IOException, InterruptedException {
		 * 
		 * double average = total/map.size();
		 * 
		 * for(String personId : map.keySet()) {
		 * 
		 * if(map.get(personId) > average) { con.write(new Text(personId), new
		 * IntWritable(map.get(personId))); } }
		 * 
		 * }
		 */
		 
			
			
			
		}
		
		
		
	
}
