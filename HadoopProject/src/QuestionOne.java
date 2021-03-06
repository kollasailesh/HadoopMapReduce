	import java.io.BufferedReader;
	import java.io.File;
	import java.io.FileReader;
	import java.io.IOException;
	import java.net.URI;
	import java.util.ArrayList;
	import java.util.Collections;
	import java.util.Comparator;
	import java.util.HashMap;

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

	public class QuestionOne {

		public static class Map
		extends Mapper<LongWritable, Text, Text, Text>{

			private final static IntWritable one = new IntWritable(1);
			private Text word = new Text();  // type of output key
			private Text Address = new Text("Palo Alto");

			public void map(LongWritable key, Text value, Context context
					) throws IOException, InterruptedException {
				String[] mydata = value.toString().split("\\^");
				if (mydata[1].contains("Palo Alto")){
					word.set(mydata[0]);
					context.write(word, Address);
				}
			}
		}

		// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: WordCount <in> <out>");
				System.exit(2);
			}

			// create a job with name "QuestionOne" 
			Job job = new Job(conf, "QuestionOne"); 
			job.setJarByClass(QuestionOne.class); 
			job.setMapperClass(Map.class);

			// set output key type 
			job.setOutputKeyClass(Text.class);
			// set output value type 
			job.setOutputValueClass(Text.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
