package edu.upenn.cis.cis455.indexer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    private static int numberOfDocuments = 0;

    public static void main(String[] args){
        Job job = null;
        Configuration config = new Configuration();
        //this is where you will be storing the correct variable for the total number of documents
        //Classical input stream usage
        config.setInt("numberOfDocuments",10000);
        try {
            job = Job.getInstance();


        } catch (IOException e) {
            e.printStackTrace();
        }
        if(job != null) {
            job.setJarByClass(Driver.class);
        } else {
            System.exit(0);
        }
        try {
            FileInputFormat.addInputPath(job, new Path(args[0])); //S3 folder name -> Extracts all files from it
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }

        //For now we will be storing back into some sort of S3 file, but eventually will be put into MySQL
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //S3 folder name -> output folder should not exist
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


    }

    public int getNumberOfDocuments(){
        return numberOfDocuments;
    }
}
