package com.hadoop.dijkstra;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class DijkstraPartitioner implements Partitioner<LongWritable, Text> {

	public int getPartition(LongWritable key, Text value, int numReduceTasks) {
		
		if(numReduceTasks==0)
			return 0;

	    int returnValue = (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;

	    return returnValue;
	}
	
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}
	

}
