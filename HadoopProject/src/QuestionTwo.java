	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileReader;
	import java.io.IOException;
	import java.net.URI;
	import java.util.ArrayList;
	import java.util.Collections;
	import java.util.Comparator;
	import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.Mapper.Context;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.util.GenericOptionsParser;

	public class QuestionTwo {

		public static class Map
		extends Mapper<LongWritable, Text, Text, DoubleWritable>{

			public void map(LongWritable key, Text value, Context context
					) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("\\^");
					if(mydata.length>3){
					context.write(new Text(mydata[2]), new DoubleWritable(Double.parseDouble(mydata[3])));  // create a pair <businesID, rating>
					}
			}
		}

		public static class Reduce
		extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
			TreeMap<Double, List> repToRecordMap = new TreeMap<Double, List>(Collections.reverseOrder());
//			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<DoubleWritable> values,Context context) 
					throws IOException, InterruptedException {
				Text tempObj  = new Text(key);
				Double sum = 0.0; // initialize the sum for each keyword
				int count = 0;
				for (DoubleWritable val : values) {
					sum += val.get();
					count ++;
				}
				Double avg = sum/count;
				if(repToRecordMap.containsKey(avg)){
					repToRecordMap.get(avg).add(tempObj);
				}else{
					List<Text> businessIds = new ArrayList<Text>();
					businessIds.add(tempObj);
					repToRecordMap.put(avg,businessIds);
				}
			}
			
			protected void cleanup(Context context) throws IOException, InterruptedException {  
				// Output our ten records to the reducers 
				Set <Double> keys = repToRecordMap.keySet();
				int count = 0;
				boolean flag = false;
				for (Double key : keys) {
					List<Text> businessIds = repToRecordMap.get(key);
					ListIterator<Text> businessIdsiterator =  businessIds.listIterator();
					while(businessIdsiterator.hasNext()){
						if(count<10){
							context.write(businessIdsiterator.next(),new DoubleWritable(key));
							count++;
						}else{
							flag = true;
						break;
						}
					}
					if(flag) break;
					} 
			}
		}

		// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: Question2 <in> <out>");
				System.exit(2);
			}

			// create a job with name "Top10AvgRatings" 
			Job job = new Job(conf, "Top10AvgRatings"); 
			job.setJarByClass(QuestionTwo.class); 
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

			// set output key type 
			job.setOutputKeyClass(Text.class);
			// set output value type 
			job.setOutputValueClass(DoubleWritable.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
