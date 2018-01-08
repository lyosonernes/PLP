package cs.bigdata.TFIDF;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// Simple WordCount Mapper:
	// -Output key contains the word and the docid(filename) seperated by a separator "--separator--"
	// -Output value is equal to one
	private final static IntWritable one = new IntWritable(1);

	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException
	{
		String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
		String line = text.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens())
		{
			Text keyOutput = new Text(tokenizer.nextToken() + "--separator--"+  fileName);
			context.write(keyOutput, one);
		}
	}
}
