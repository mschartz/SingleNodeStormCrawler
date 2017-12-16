package edu.upenn.cis.cis455.indexer;


import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) {
        if(key == null)
        {
            return;
        }
        //Value = "A,1,B,C"
        String UrlAndDoc = value.toString();
        System.out.println("MAP___Line : "+UrlAndDoc);
        if(!UrlAndDoc.contains("--@--@"))
        {
            System.err.println("MAP___No delimiters in line");
            return;
        }

        String[] splitLine = UrlAndDoc.split("--@--@");
        String URL = splitLine[0];
        String document = splitLine[1];
        Stemmer stemmer = new Stemmer();
        String[] words = document.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
        HashMap<String, Integer> frequencies = new HashMap<>();
        int maxFrequency = 0;
        for(String word : words){
            word = stemmer.stem(word);
            if(!frequencies.containsKey(word)){
                frequencies.put(word, 1);
                if(maxFrequency == 0) maxFrequency = 1;
            } else {
                int currentCount = frequencies.get(word);
                currentCount++;
                if(currentCount > maxFrequency) maxFrequency = currentCount;
                frequencies.put(word, currentCount);
            }
        }
        HashMap<String, Double> tfScore = new HashMap<>();
        Iterator<String> wordIterator = frequencies.keySet().iterator();
        while(wordIterator.hasNext()){
            String freqKey = wordIterator.next();
            int count = frequencies.get(freqKey);
            double tf = .5  + .5 * (double) count / maxFrequency;
            tfScore.put(freqKey, tf);
        }

        Iterator<String> tfScoreIterator = tfScore.keySet().iterator();
        while(tfScoreIterator.hasNext()){
            String currentKey = tfScoreIterator.next();
            double tf  = tfScore.get(currentKey);
            Text outputKey = new Text(currentKey);
            Text outputVal = new Text(tf + "--@--@" + URL);
            try {
                context.write(outputKey, outputVal);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }




//        String pr = new String(pageRank.toString()); //1st two are the link itself and it's page rank
//        System.out.println("MAP___Page Rank: "+pr);
//        Text emitValue = new Text(line);
//        try {
//            //Emit the original line
//            System.out.println("MAP___Emitting: "+line);
//            context.write(emitKey, emitValue);
//
//            //Emit the divided age rank
//            for(int position = 2; position < values.length; position++)
//            {
//                System.out.println("MAP___PR for "+values[position]);
//                context.write(new Text(values[position]), new Text(pr));
//            }
//        } catch (IOException | InterruptedException e) {
//            e.printStackTrace();
//        }
    }
    public static void main(String[] args){
        Stemmer stem = new Stemmer();
        System.out.println(stem.stem("running"));
        System.out.println(stem.stem("screaming"));
    }
}
