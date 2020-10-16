//Assignment No: 1
//Name: Rasika Hedaoo
//Student ID: 1001770527

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;

public class Graph {
//First Mapper Starts
//Pseudo code
//map ( key, line ):
//read 2 long integers from the line into the variables key2 and value2
//emit (key2,value2)
	public static class EdgeCounterMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//read lines seperated by commas
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long graph_node = s.nextLong();
			long graph_edge_node = s.nextLong();
			//emit (key2,value2)
			context.write(new LongWritable(graph_node), new LongWritable(graph_edge_node));
			s.close();
		}
	}
//First Mapper Ends

//First Reducer Starts
//reduce ( key, nodes ):
 //count = 0
//for n in nodes
//count++
//emit(key,count)
	public static class EdgeCounterReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable v : values) {
				count++;
			}
			context.write(key, new LongWritable(count));	//output is saved in tmp folder
		}
	}
//First Reducer Ends

//Second Mapper Starts
//map ( node, count ):
//emit(count,1)
	public static class NodeGrouperMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//read lines seperated by commas
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long graph_node = s.nextLong();
			long edge_count = s.nextLong();
			//emit(count,1)
			context.write(new LongWritable(edge_count), one);
			s.close();
		}
	}
//Second Mapper Ends

//Second Reducer Starts
//reduce ( key, values ):
//sum = 0
//for v in values
//    sum += v
//emit(key,sum)
	public static class NodeGrouperReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v : values) {
				sum += v.get();
			}
			//emit(key,sum)
			context.write(key, new LongWritable(sum));
		}
	}
//Second Reducer Ends
	
//Main Starts for both the Mappers and Reducers
//Exception Handler
	public static void setJobConfigurations(Job job) throws Exception {
		job.setJarByClass(Graph.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	}

//Job for Mapper and Reducer One
	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("EdgeCount");
		setJobConfigurations(job1);
		Configuration conf = job1.getConfiguration();
		conf.set("mapred.textoutputformat.separator", ",");
		job1.setMapperClass(EdgeCounterMapper.class);
		job1.setReducerClass(EdgeCounterReducer.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("tmp")); //a tmp folder is created for storing the result of Mapper Reducer One
		job1.waitForCompletion(true);
		
//Job for Mapper and Reducer 2
		Job job2 = Job.getInstance();
		job2.setJobName("NodeCount");
		setJobConfigurations(job2);
		conf = job2.getConfiguration();	
		conf.set("mapred.textoutputformat.separator", " ");	//set the output in the form of space seperated elements
		job2.setMapperClass(NodeGrouperMapper.class);
		job2.setReducerClass(NodeGrouperReducer.class);
		FileInputFormat.setInputPaths(job2, new Path("tmp")); //tmp folder for generated result of Mapper and Reducer One
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.waitForCompletion(true);
	}
}