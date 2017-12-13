package edu.upenn.cis.cis455.page_rank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.upenn.cis.cis455.mapreduce.MapperClass;
import edu.upenn.cis.cis455.mapreduce.ReducerClass;

/**
 * Hello world!
 *
 *
 *S3 bucket: 	
 *	--> Input folder
 *	--> Output folder
 * 	--> Jar folder
 */
public class Driver 
{
	public static void main( String[] args )
	{
		Job job = null;
		try {
			job = new Job();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		job.setJarByClass(Driver.class);  
		try {
			FileInputFormat.addInputPath(job, new Path(args[0])); //S3 folder name -> Extracts all files from it
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1])); //S3 folder name -> output folder should not exist
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);  
		//File system api hdfs api to:
//		1. Copy the output folder to input folder
//		2. Rename the file
//		3. Delete the output folder
		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
