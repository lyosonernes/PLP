package cs.bigdata.TFIDF;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class SortTFIDFMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	// Mapper for sorting TFIDF
	// -Output key is the Tf-IDF
	// -Output value contains the word and docid seperated by a separator "--separator--"
	// Overriding of the map method
	protected void map(LongWritable key, Text TFIDF, Context context) throws IOException,InterruptedException
	{
		int tabIndex = TFIDF.find("\t");
		String formerKey = Text.decode(TFIDF.getBytes(),0,tabIndex);
		String formerValues = Text.decode(TFIDF.getBytes(),tabIndex +1, TFIDF.getLength()-(tabIndex+1));
		DoubleWritable keyOutput = new DoubleWritable(-Double.parseDouble(formerValues));
		context.write(keyOutput, new Text(formerKey));

	}
}
