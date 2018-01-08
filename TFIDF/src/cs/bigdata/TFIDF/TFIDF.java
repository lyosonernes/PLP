package cs.bigdata.TFIDF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class TFIDF extends Configured implements Tool {


	public int run(String[] args) throws Exception {

		if (args.length != 3) {

			System.out.println("Usage: [input1] [input2] [output]");

			System.exit(-1);

		}
		Path outputWordCount = new Path(args[2]+"/wordCount");
		Path outputWordPerDoc = new Path(args[2]+"/wordCountPerDoc"); 
		Path outputTFIDF = new Path(args[2]+"/TFIDF");
		Path outputTFIDFSorted = new Path(args[2]+"/TFIDFSorted");

		//Delete files if already existing

		FileSystem fsWordCount = FileSystem.newInstance(getConf());

		if (fsWordCount.exists(outputWordCount)) {

			fsWordCount.delete(outputWordCount, true);

		}
		int jobDone1 = wordCountJob(new Path(args[0]), new Path(args[1]), outputWordCount);
		int jobDone2 = wordCountPerDocJob(outputWordCount, outputWordPerDoc);
		int jobDone3 = TFIDFJob(outputWordPerDoc, outputTFIDF);
		int jobDone4 = TFIDFSortedJob(outputTFIDF, outputTFIDFSorted);
		
		return jobDone4;

	}

	public int wordCountJob(Path pathIn1, Path pathIn2, Path outputWordCount) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {
		// First job for WordCount
		Job jobWordCount = Job.getInstance(getConf());

		jobWordCount.setJobName("WordCount");


		// Mapper and Reducer configs

		jobWordCount.setJarByClass(TFIDF.class);

		jobWordCount.setMapperClass(WordCountMapper.class);

		jobWordCount.setReducerClass(WordCountReducer.class);


		// Definition of type for key/value

		jobWordCount.setMapOutputKeyClass(Text.class);

		jobWordCount.setMapOutputValueClass(IntWritable.class);


		jobWordCount.setOutputKeyClass(Text.class);

		jobWordCount.setOutputValueClass(Text.class);


		// Define I/O manager

		FileInputFormat.addInputPath(jobWordCount, pathIn1);
		FileInputFormat.addInputPath(jobWordCount, pathIn2);

		FileOutputFormat.setOutputPath(jobWordCount, outputWordCount);


		int codeWordCount = jobWordCount.waitForCompletion(true) ? 0: 1;

		return codeWordCount;

	}

	public int wordCountPerDocJob(Path input, Path output) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {

		// Second job for WordCountPerDoc
		Job jobWordCountPerDoc = Job.getInstance(getConf());

		jobWordCountPerDoc.setJobName("WordCountPerDoc");


		// Mapper and Reducer configs

		jobWordCountPerDoc.setJarByClass(TFIDF.class);

		jobWordCountPerDoc.setMapperClass(WordCountPerDocMapper.class);

		jobWordCountPerDoc.setReducerClass(WordCountPerDocReducer.class);


		// Definition of type for key/value

		jobWordCountPerDoc.setMapOutputKeyClass(Text.class);

		jobWordCountPerDoc.setMapOutputValueClass(Text.class);


		jobWordCountPerDoc.setOutputKeyClass(Text.class);

		jobWordCountPerDoc.setOutputValueClass(Text.class);


		// Define I/O manager

		FileInputFormat.setInputPaths(jobWordCountPerDoc, input);
		FileOutputFormat.setOutputPath(jobWordCountPerDoc, output);

		//Delete files if already existing

		FileSystem fsWordCountPerDoc = FileSystem.newInstance(getConf());

		if (fsWordCountPerDoc.exists(output)) {

			fsWordCountPerDoc.delete(output, true);

		}

		int codeWordCountPerDoc = jobWordCountPerDoc.waitForCompletion(true) ? 0: 1;

		return codeWordCountPerDoc;
	}

	public int TFIDFJob(Path input, Path output) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {
		// Third job for TFIDF
		Job TFIDFDriver = Job.getInstance(getConf());

		TFIDFDriver.setJobName("TFIDF");


		// Mapper and Reducer configs

		TFIDFDriver.setJarByClass(TFIDF.class);

		TFIDFDriver.setMapperClass(TFIDFMapper.class);

		TFIDFDriver.setReducerClass(TFIDFReducer.class);


		// Definition of type for key/value

		TFIDFDriver.setMapOutputKeyClass(Text.class);

		TFIDFDriver.setMapOutputValueClass(Text.class);


		TFIDFDriver.setOutputKeyClass(Text.class);

		TFIDFDriver.setOutputValueClass(DoubleWritable.class);


		// Define I/O manager

		FileInputFormat.setInputPaths(TFIDFDriver, input);
		FileOutputFormat.setOutputPath(TFIDFDriver, output);


		//Delete files if already existing

		FileSystem fsTFIDF = FileSystem.newInstance(getConf());

		if (fsTFIDF.exists(output)) {

			fsTFIDF.delete(output, true);

		}

		return TFIDFDriver.waitForCompletion(true) ? 0: 1;
	}
	
	public int TFIDFSortedJob(Path input, Path output) throws IOException, 
	ClassNotFoundException, 
	InterruptedException {
		// Fourth job for TFIDFSorted
		Job TFIDFSortedDriver = Job.getInstance(getConf());

		TFIDFSortedDriver.setJobName("TFIDFSorted");


		// Mapper and Reducer configs

		TFIDFSortedDriver.setJarByClass(TFIDF.class);

		TFIDFSortedDriver.setMapperClass(SortTFIDFMapper.class);


		// Definition of type for key/value

		TFIDFSortedDriver.setMapOutputKeyClass(DoubleWritable.class);

		TFIDFSortedDriver.setMapOutputValueClass(Text.class);


		// Define I/O manager

		FileInputFormat.setInputPaths(TFIDFSortedDriver, input);
		FileOutputFormat.setOutputPath(TFIDFSortedDriver, output);


		//Delete files if already existing

		FileSystem fsTFIDF = FileSystem.newInstance(getConf());

		if (fsTFIDF.exists(output)) {

			fsTFIDF.delete(output, true);

		}

		return TFIDFSortedDriver.waitForCompletion(true) ? 0: 1;
	}
	public static void main(String[] args) throws Exception {

		TFIDF TFIDFDriver = new TFIDF();

		int res = ToolRunner.run(TFIDFDriver, args);

		System.exit(res);

	}

}

