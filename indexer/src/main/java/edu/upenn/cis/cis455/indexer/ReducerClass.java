package edu.upenn.cis.cis455.indexer;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class ReducerClass extends Reducer<LongWritable, Text, Text, Text> {

    private int numberOfDocuments;
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        numberOfDocuments = config.getInt("numberOfDocuments", 0);
    }
    public void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException
    {

        //Add all the Page Ranks multiple by B(0.85) and add A(0.15)
        String word = key.toString();
        HashMap<String, Double> map = new HashMap<>();
        int appearances = 0;
        for(Text v : values) {
            String[] current = v.toString().split("--@--@");
            map.put(current[1], Double.parseDouble(current[0]));
            appearances++;
        }

        double idf = Math.log((double) numberOfDocuments/ appearances);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(idf);
        stringBuilder.append('\t');

        Iterator<String> keysIterator = map.keySet().iterator();
        while(keysIterator.hasNext()){
            String currentKey = keysIterator.next();
            stringBuilder.append(currentKey);
            stringBuilder.append('\t');
            double tfidf = map.get(currentKey) * idf;
            stringBuilder.append(tfidf);
            stringBuilder.append('\t');
        }

        Text outputKey = new Text(word);
        Text outputValue = new Text(stringBuilder.toString());
        context.write(outputKey, outputValue);
    }

}
