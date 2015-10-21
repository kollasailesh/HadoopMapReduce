

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class QuestionFour 
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		HashSet<String> business_id_set = null;
		@Override
		public void setup(Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			business_id_set = new HashSet<String>();
			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("myfilepath");
			Path part=new Path("hdfs://cshadoop1"+myfilepath);//Location of file in HDFS
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) 
		    {
		        Path pt = status.getPath();
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null)
		        {
		        	String[] businnes_data = line.split("\\^");
		            if(businnes_data[1].contains("Stanford"))
		            {
		            	business_id_set.add(businnes_data[0]);
		            }
		            line=br.readLine();
		        }
		       
		    }
	       
	    }

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] mydata = value.toString().split("\\^");
			if(business_id_set.contains(mydata[2]))
			{
				context.write(new Text(mydata[1]),new Text(mydata[3]));
			}
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("myfilepath", otherArgs[0]);
		if (otherArgs.length != 3) {
			System.err.println("Usage: Question4 <in 1> <in 2> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "QuestionFour"); 
		job.setJarByClass(QuestionFour.class); 
		job.setMapperClass(Map.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}