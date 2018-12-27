
package FrontEnd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MapReduceShell extends Configured implements Tool {
	
 	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
 	{	
 		
 		public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException
 		{
 			
 		}//end map
 		
	}//end MyMapper
 		
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
		}//end reduce
		
	}//end MyReducer
 	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new MapReduceShell(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception 
	{
		int numReduces = 1;
		
		Path inputPath = new Path("someInputDir");
		Path outputPath = new Path("someOutputDir");
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "MapReduceShell Test");
		
		job.setNumReduceTasks(numReduces);
		job.setJarByClass(MapReduceShell.class);
		
		//sets mapper class
		job.setMapperClass(MyMapper.class);
		
		//sets map output key/value types
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
		//Set Reducer class
	    job.setReducerClass(MyReducer.class);
	    
	    // specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		//sets Input format
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    // specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		job.waitForCompletion(true);
		return 0;
	}
 	
}
