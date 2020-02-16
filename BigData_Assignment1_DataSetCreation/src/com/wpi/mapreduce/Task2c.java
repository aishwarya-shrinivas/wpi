package com.wpi.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

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


public class Task2c {
	

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "wordcount");
		j.setJarByClass(Task2c.class);
		j.setMapperClass(MapForWordCount.class);
		j.setReducerClass(ReduceForWordCount.class);
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
			
				
				con.write(new Text(words[2]), new IntWritable(1));
			}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private HashMap<String, Integer>map = new HashMap<String, Integer>();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			
			
			int sum = 0;
			for (IntWritable val : values) {
                sum += val.get();
            }
			
			map.put(key.toString(), sum);
			con.write(new Text(key+" :reducer"),new IntWritable(sum));
			
			
		}
		
		 public void cleanup(Context con) throws IOException, InterruptedException {
			  	
		    con.write(new Text("General Information:"), new IntWritable(map.size())); 
				  
		    int count = 1;
		    
		    final Map<String, Integer> sortedByCount = sortByValue(map);
		    
		    for(String person : sortedByCount.keySet()) {
		    	con.write(new Text(person),new IntWritable(sortedByCount.get(person)));
		    	
		    	if(count == 10) {
		    		break;
		    	}
		    	count++;
		    }
		 
		 
		 }
	
}
	
	
	

    public static Map<String, Integer> sortByValue(final Map<String, Integer> wordCounts) {
        return wordCounts.entrySet()
                .stream()
                .sorted((Map.Entry.<String, Integer>comparingByValue().reversed()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }	
}
