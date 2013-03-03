/**
 * 
 */
package com.arjun.ii;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * @author arjunflex
 *
 */
public class InvertedIndex {
	
	public static class InvertedIndexMap extends 
				Mapper<LongWritable, Text, Text, Text> {
		private Text documentId;
		private Text word = new Text();
		
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			String fileName = 
					((FileSplit)context.getInputSplit()).getPath().getName();
			documentId = new Text(fileName);
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word,documentId);
				
			}
		}
		
	}
	
	public static class InvertedIndexReduce 
			extends Reducer<Text, Text, Text, Text> {
		private Text docIds = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			
			HashSet<Text> uniqueIds = new HashSet<Text>();
			StringBuffer buffer = new StringBuffer();
			for(Text docId: values) {
				
				if(!uniqueIds.contains(docId)) {
						buffer.append(docId).append(" ");
						uniqueIds.add(new Text(docId));
				}
				
			}
			
			docIds.set(new Text(buffer.toString()));
			context.write(key, docIds);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		runJob(
				args[0], 
				args[args.length - 1]);
		
		
	}
	
	public static void runJob(String input , String output) 
		throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMap.class);
		job.setReducerClass(InvertedIndexReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		job.waitForCompletion(true);
		
		
	}
}
