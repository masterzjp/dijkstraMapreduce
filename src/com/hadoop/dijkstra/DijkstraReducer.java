package com.hadoop.dijkstra;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DijkstraReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterator<Text> values,
			OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

		int min = Integer.MAX_VALUE;
		String vertices = "";
		
		//print valores de entrada
		System.out.println("input reduce key " + key);
		 
		while (values.hasNext())
	    {
			String[] tokens = values.next().toString().split("\t");
			System.out.println("tokens reduce " + Arrays.toString(tokens));
			
			if(tokens[0].equals("Vertices")){
				vertices = tokens[1];
			}
			else if(tokens[0].equals("dist")){
				min = Math.min(Integer.parseInt(tokens[1]), min);
			}
	    }
	    output.collect(key, new Text(min + "\t" + vertices));
	    System.out.println("vertices reduce " + vertices);
	}

}
