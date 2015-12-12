package com.hadoop.dijkstra;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DijkstraMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
	private static final int MINIMUM_NODE_TOKENS = 3;
	
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
		
		String[] tokens = value.toString().split("\t");
		
		System.out.println("tokens input map " + Arrays.toString(tokens));
		
		if(tokens.length >= MINIMUM_NODE_TOKENS){
	        String[] adjacentNodeDetails = tokens[2].split(" ");
	        String[][] neighborsNode = new String[adjacentNodeDetails.length][2];

	        for (int index = 0; index < adjacentNodeDetails.length; index++)
	        {
	        	neighborsNode[index] = adjacentNodeDetails[index].substring(1, adjacentNodeDetails[index].length() - 1).split(",");
	        }
	        int sourceNodeWeight = Integer.parseInt(tokens[1]);
	        //recorriendo los nodos vecinos
	        for (int index = 0; index < adjacentNodeDetails.length; index++)
	        {
	          int weight = sourceNodeWeight + Integer.parseInt(neighborsNode[index][1]); //Nueva distancia desde el origen hacia los nodos vecinos
	          
	          output.collect(new LongWritable(Integer.parseInt(neighborsNode[index][0])), new Text("dist"+ "\t" + weight));
	          System.out.println("vecinos map key: " + neighborsNode[index][0]);
	          System.out.println("vecinos map value: " + new Text("dist " + weight));
	        }
	   }
		
		output.collect(new LongWritable(Integer.parseInt(tokens[0])), new Text("dist"+ "\t" + tokens[1]));
		
		if(tokens.length >= MINIMUM_NODE_TOKENS){
			output.collect(new LongWritable(Integer.parseInt(tokens[0])), new Text("Vertices"+ "\t" + tokens[2]));
			System.out.println("map value vertices: " + new Text("Vertices "+ tokens[2]));
		}
		
		//print salida 
		System.out.println("map value dist: " + new Text("dist "+ tokens[1]) );
		
		
	}

}
