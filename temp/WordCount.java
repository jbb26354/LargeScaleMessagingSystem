
package FrontEnd;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/*
 * This program is designed to find the count of every word in an input file set.
 */

public class WordCount extends Configured implements Tool {
		
 	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
 	{	
 		
 		public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException
 		{
 			//The inputValue is going to be a line of text for our program.
 			//We will be manipulating this line of text to get every word out of it and then output it.
 			//A call of the map method will be invoked for every line of text in the input files.
 			
 			//Create a string tokenizer from the input file so that we can 
 			//easily output all of the "word" in this input 
 			StringTokenizer line = new StringTokenizer(inputValue.toString());
 			
 			/*
 			 * for each "word" or token in the input string, output that word with a count of "1". 
 			 * We will add these words up later in the reduce. 
 			 * We can also write a combiner class if we want to help combine some of these values 
 			 * before we transmit the information over the network. This will not however be done 
 			 * for simplicities sake.
 			 */
 			while(line.hasMoreTokens())
 			{
 				//this writes the Key Value pair to the system so that it can be shuffled 
 				//around and then sent to the appropriate reduce group.
 				//The key in this program is going to be the word, this will cause all counts 
 				//of a word to be sent to the same place. The value will be '1' in this case.
 				context.write(new Text(line.nextToken()), new IntWritable(1));
 			}
 			
 			
 			
 		}//end map
 		
	}//end MyMapper

	public static class MyReducer extends Reducer<Text, IntWritable, Text, Text>
	{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			//create a sum that we can add up values into.
			long sum = 0;
			
			//create the interator to iterate over the Iterable list values
			Iterator<IntWritable> valItr = values.iterator();
			
			//While there are more values to process, get them and add them to our sum
			while(valItr.hasNext())
			{
				//Get the next IntWritable and then, from that get 
				//it's value so that we can add it to the sum
				sum += valItr.next().get();
				
			}
			
			//Since we now have the sum of all the counts of a specific word, we can output this as our result.
			
			//create a new text object with the sum as it's value
			//output the key and the sum as a result. 
			context.write(key, new Text("" + sum));
			
			
		}//end reduce
		
	}//end MyReducer
 	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception 
	{
		
		if(args.length != 3)
		{
			System.out.println("Usage: bin/hadoop jar MapReduceSample.jar WordCount <input directory> <ouput directory> <number of reduces>");
			System.out.println("args length incorrect, length: " + args.length);
			return -1;
		}
		int numReduces;
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		try
		{
			numReduces 	= new Integer(args[2]);
			System.out.println("number reducers set to: " + numReduces);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Usage: bin/hadoop jar MapReduceSample.jar WordCount <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: number of reduces not a type integer");
			return -1;
		}
		
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(inputPath))
		{
			System.out.println("Usage: bin/hadoop jar MapReduceSample.jar WordCount <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: Input Directory Does Not Exist");
			System.out.println("Invalid input Path: " + inputPath.toString());
			return -1;
		}
		
		if(fs.exists(outputPath))
		{
			System.out.println("Usage: bin/hadoop jar MapReduceSample.jar WordCount <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: Output Directory Already Exists");
			System.out.println("Please delete or specifiy different output directory");
			return -1;
		}
		
		
		conf.set("mapred.child.java.opts", "-Xmx512M");
		conf.setBoolean("mapred.output.compress", false);
		conf.set("mapred.task.timeout", "8000000");
		
		Job job = new Job(conf, "Word Count");
		
		job.setNumReduceTasks(numReduces);
		job.setJarByClass(WordCount.class);
		
		//sets mapper class
		job.setMapperClass(MyMapper.class);
		
		//sets map output key/value types
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
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
