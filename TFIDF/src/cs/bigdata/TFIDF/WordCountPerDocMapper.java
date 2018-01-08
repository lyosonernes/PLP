package cs.bigdata.TFIDF;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class WordCountPerDocMapper extends Mapper<LongWritable, Text, Text, Text> {
	// Mapper for getting the count of words per doc
	// - Output key is the docid 
	// - Output value contains the word and the wordcount associated seperated by a separator "--separator--"
	// Overriding of the map method
	protected void map(LongWritable key, Text wordCount, Context context) throws IOException,InterruptedException
	{
		int tabIndex = wordCount.find("\t");
		String formerKey = Text.decode(wordCount.getBytes(),0,tabIndex);
		String formerValues = Text.decode(wordCount.getBytes(),tabIndex +1, wordCount.getLength()-(tabIndex+1));
		String [] keyValueArray = formerKey.split("--separator--");
		Text valueOutput = new Text(keyValueArray[0] + "--separator--" + formerValues);
		context.write(new Text (keyValueArray[1]), valueOutput);

	}
}
