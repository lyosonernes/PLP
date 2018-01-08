package cs.bigdata.TFIDF;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
	// Mapper for getting the TfIDF
	// -Output key is the word
	// -Output value contains the docid, wordCount and wordsPerDoc seperated by a separator "--separator--"
	
	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text wordCount, Context context) throws IOException,InterruptedException
	{
		// Get former key and value from the previous mapreduce
		int tabIndex = wordCount.find("\t");
		String formerKey = Text.decode(wordCount.getBytes(),0,tabIndex);
		String formerValues = Text.decode(wordCount.getBytes(),tabIndex +1, wordCount.getLength()-(tabIndex+1));
		String [] keyTextArray = formerKey.split("--separator--");
		String [] wordCountTextArray = formerValues.split("--separator--");
		
		Text valueOutput = new Text(keyTextArray[1] + "--separator--"+ wordCountTextArray[0] + "--separator--" + wordCountTextArray[1]);
		context.write(new Text(keyTextArray[0]), valueOutput);
	}
}

