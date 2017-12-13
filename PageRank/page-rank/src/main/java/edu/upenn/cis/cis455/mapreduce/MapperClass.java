package edu.upenn.cis.cis455.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) {
	     
		if(key == null)
		{
			return;
		}
		//Value = "A,1,B,C"
		String line = value.toString();
		System.out.println("MAP___Line : "+line);
		if(!line.contains("\t"))
		{
			System.err.println("MAP___No delimiters in line");
			return;
		}
		
		String[] values = line.split("\t");
		Text emitKey = new Text(values[0]);
		Double pageRank = new Double((Double.parseDouble(values[1])/(values.length - 2.0)));
		String pr = new String(pageRank.toString()); //1st two are the link itself and it's page rank
		System.out.println("MAP___Page Rank: "+pr);
		Text emitValue = new Text(line);
		try {
			//Emit the original line
			System.out.println("MAP___Emitting: "+line);
			context.write(emitKey, emitValue);
			
			//Emit the divided age rank
			for(int position = 2; position < values.length; position++)
			{
				System.out.println("MAP___PR for "+values[position]);
				context.write(new Text(values[position]), new Text(pr));
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	    }


}
