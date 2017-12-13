package edu.upenn.cis.cis455.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class ReducerClass extends Reducer <Text, Text, Text, Text> {

	private static final double A = 0.15;
	private static final double B = 0.85;
	public void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException 
	{
		
		//Add all the Page Ranks multiple by B(0.85) and add A(0.15)
		List<String> vArrayList = new ArrayList<String>();
    	
    	for(Text v : values) {
    		vArrayList.add(v.toString());
    	}
    	String sKey = key.toString();
    	Double pr = 0.0;
    	String outgoingLinks = "";
    	for(String s: vArrayList)
    	{
    		
    		if(s.startsWith(sKey))
    		{
    			//Extract the outgoing links from the line
    			//Value = a,1,b,c; need to extract the outgoing links
    			if(s.contains("\t"))
    			{
    				String[] vals = s.split("\t");
    				if(vals.length > 2 ) {
    					outgoingLinks = vals[2];
	    				for(int pos = 3; pos < vals.length; pos++)
	    				{
	    					outgoingLinks +="\t"+vals[pos];
	    					System.out.println("RED___Page Rank incremented = "+ pr);
	    				}
	    				System.out.println("RED___Outgoing links: "+outgoingLinks);
    				}
    			}
    		}
    		else
    		{
    			//Formula: A +B(Incoming PR)
    			pr += Double.parseDouble(s);
    		}
    	}
    	pr = A+B*pr;
    	System.out.println("RED___Page Rank computed = "+ pr);
    	String pageRank = pr.toString();
    	Text emitValue = /*key.toString() +*/new Text(pageRank+ "\t"+ outgoingLinks);
		System.out.println("RED___Emitting value: "+emitValue.toString());
    	context.write(key, emitValue);
	}

}
