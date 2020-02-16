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
		con.write(new Text(key+" :reducer"),new IntWritable(sum));
	}
    }
    
    
    public void cleanup(Context con) throws IOException, InterruptedException {
		  
    	//double average = total/map.size();
    	
    	con.write(new Text("General:"+total), new IntWritable(map.size())); 
		  
		  for(String personId : map.keySet()) {
		  
		// if(map.get(personId) > average) { 
			 con.write(new Text(personId+": cleanup"), new IntWritable(map.get(personId))); } }
		
			  

		  //   }
		  
 	
 }
     
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
     
    Job job = new Job(conf, "wordcount");
     
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

