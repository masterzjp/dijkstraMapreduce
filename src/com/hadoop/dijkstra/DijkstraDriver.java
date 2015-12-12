package com.hadoop.dijkstra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DijkstraDriver extends Configured implements Tool{
	
	private static final int    MINIMUM_NODE_TOKENS = 3;
	
	public int run(String[] args) throws Exception {
		long startTime = System.currentTimeMillis(); 
		Options options = new Options();
		options.addOption("i", true, "Input Directory Path");
		options.addOption("o", true, "Output Directory Path");
		options.addOption("m", true, "Number of Map tasks");
		options.addOption("r", true, "Number of Reduce Tasks");
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		
		if (!cmd.hasOption("i")) {
			System.out
					.println("Please specify the input path using the -i option");
			return 1;
		}
		if (!cmd.hasOption("o")) {
			System.out
					.println("Please specify the output path using the -o option");
			return 1;
		}
		
		int counter = 0;
	    String inputPath = cmd.getOptionValue("i");
	    String outputPath = cmd.getOptionValue("o");
	    String output = outputPath + counter;
		int max = 0;
	    do {
	    	JobConf conf = new JobConf(getConf(), DijkstraDriver.class);
	        conf.setJobName("parallel Dijkstra");
	          
	        //Nombre de las clases de mapper y reduce
	 		conf.setMapperClass(DijkstraMapper.class);
	 		conf.setReducerClass(DijkstraReducer.class);

	        //Formato para dividir el archivo de entrada
	        conf.setInputFormat(TextInputFormat.class);
	          
	        //Tipo de dato de salida clave, valor
	        conf.setOutputKeyClass(LongWritable.class);
	        conf.setOutputValueClass(Text.class);
	        
	        //Jobconf activar/desactivar Speculative Execution
	        conf.setSpeculativeExecution(false);     //conf.setBoolean("mapreduce.map.speculative", false); conf.setBoolean("mapreduce.reduce.speculative", false);

	        if (cmd.hasOption("m")) {
				conf.setNumMapTasks(Integer.parseInt(cmd.getOptionValue("m"))); // Task map
			}
			if (cmd.hasOption("r")) {
				conf.setNumReduceTasks(Integer.parseInt(cmd.getOptionValue("r"))); // Task reduce
			}

	        //Estableciendo los directorios de entrada y salida	         
		    FileInputFormat.setInputPaths(conf, new Path(inputPath));
		    FileOutputFormat.setOutputPath(conf, new Path(output));
		    
			//Nombre de la clase de particion
		    conf.setPartitionerClass(DijkstraPartitioner.class);
		  
		    try {
		    	 JobClient.runJob(conf);  //Run job
			} catch (Exception e) {
				e.printStackTrace();
			}
		    
		    inputPath = output + "/part-00000";
			FileSystem hdfs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(inputPath))));
			String line = null;
			
			while((line = br.readLine())!=null){
				String[] tokens = line.split("\t");
				System.out.println("arrat driver tokens " + Arrays.toString(tokens));
				
				if(tokens.length >= MINIMUM_NODE_TOKENS){
					String[] vertices = tokens[2].split(" ");
					
					if(max < vertices.length){
						max = vertices.length; 
					}
				}
			}
			br.close();
			counter++;
			inputPath = output;
			output = outputPath + counter;
	 	  
		} while (counter <= (max + 1));
		
	    long endTime = System.currentTimeMillis();
	    System.out.println("Tiempo ejecucion: " + (endTime - startTime)/1000 + "s.");
	    
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DijkstraDriver(), args);
		System.exit(res);
	}
}
