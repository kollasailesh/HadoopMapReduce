
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class QuestionThree 
{
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{	
			String[] mydata = value.toString().split("\\^");
			context.write(new Text(mydata[2]),new DoubleWritable(Double.parseDouble(mydata[3])));
		}
	}
	
	public static class Bizid_rating_mapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] mydata = value.toString().split("\\t");
			context.write(new Text(mydata[0]),new Text(mydata[1]+" "+"0"));
		}
	}
	
	public static class Biz_table_mapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] mydata = value.toString().split("\\^");
			context.write(new Text(mydata[0]),new Text(value.toString()+" "+"1"));
		}
	}
	
	public static class Reducer2 extends Reducer<Text,Text,String,String> 
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			String tupleT1 =null;
			String tupleT2 = null;
			for (Text val : values)
			{
				String tempTuple = val.toString();
				if(tempTuple.endsWith("0"))
				{
					tupleT1 = tempTuple;
				}
				else if(tempTuple.endsWith("1"))
				{
					tupleT2 = tempTuple;
				}
				
			}
			if(tupleT1 != null)
			{
				tupleT1 = tupleT1.replaceAll("\\^", "\\t");
				tupleT2 = tupleT2.replaceAll("\\^", "\\t");
				context.write(" ", tupleT2.substring(0, tupleT2.length()-2)+ " "+ tupleT1.substring(0, tupleT1.length()-2));
			}
			
			
		}
	}

	public static class Reduce extends Reducer<Text,DoubleWritable,String,DoubleWritable> 
	{
		public static TreeMap<Double,List<String>> repToRecordMap = new TreeMap<Double, List<String>>((Collections.reverseOrder()));
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException
		{
			double sum = 0; // initialize the sum for each keyword
			int count = 0;
			for (DoubleWritable val : values) 
			{
				sum += val.get();
				count++;
			}
			double average = sum/count;
			Double doubleobj = new Double(average);
			String business_id = key.toString();
			if(repToRecordMap.get(doubleobj)!=null)
			{
				repToRecordMap.get(doubleobj).add(business_id);
			}
			else
			{
				List<String> businessidlist = new ArrayList<String>();
				businessidlist.add(business_id);
				repToRecordMap.put(doubleobj, businessidlist);
			}
			
		}
	
		@Override	
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			Set<Double> keys = repToRecordMap.keySet();
			int count = 0;
			for(Double key: keys)
			{
				List<String> businessidlist = repToRecordMap.get(key);
				ListIterator<String> businessIdListiterator = businessidlist.listIterator();
				while (businessIdListiterator.hasNext())
				{
					if(count<10)
					{
						context.write(businessIdListiterator.next(),new DoubleWritable(key));
						count++;
					}
					else
					return;
				}
        	
			}
		}
	
	}

	// Driver program
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: QuestionThree <in 1> <in 2> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "QuestionThree"); 
		job.setJarByClass(QuestionThree.class); 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("/sxk145331_tempoutput"));
		
		if(job.waitForCompletion(true) == true)
		{
			Job job2 = new Job(conf, "toptenratings");
			job2.setJarByClass(QuestionThree.class);
			
			MultipleInputs.addInputPath(job2, new Path("/sxk145331_tempoutput"),TextInputFormat.class,Bizid_rating_mapper.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),TextInputFormat.class,Biz_table_mapper.class);
			job2.setReducerClass(Reducer2.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} 
	}
		
}
