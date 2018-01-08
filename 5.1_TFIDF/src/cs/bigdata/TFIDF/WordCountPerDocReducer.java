package cs.bigdata.TFIDF;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;



public class WordCountPerDocReducer extends Reducer<Text,Text,Text,Text> {
	// Reducer for getting the count of words per doc
	// -Output key contains the word and the docid associated seperated by a separator "--separator--"
	// -Output value contains

	// Overriding of the reduce function
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
		int sum = 0;
		Iterator<Text> iteratorSum = values.iterator();
		ArrayList<String> cache = new ArrayList<String>();
		
		// First iteration to determine the number of words per doc
		while (iteratorSum.hasNext()) {
			Text value = iteratorSum.next();
			String valuesText = Text.decode(value.getBytes(),0,value.getLength());
			String [] valuesTextArray = valuesText.split("--separator--");
			sum += Integer.parseInt(valuesTextArray[1]);

			cache.add(valuesText);
		}
		
		int size = cache.size();
		// Second iteration to get the output
        for (int i = 0; i < size; ++i) {
			String valuesText = cache.get(i);
			String [] valuesTextArray = valuesText.split("--separator--");
			Text keyOutput = new Text(valuesTextArray[0]+ "--separator--"+ Text.decode(key.getBytes(), 0, key.getLength()));
			Text valueOutput = new Text(valuesTextArray[1] + "--separator--" + Integer.toString(sum));

			context.write(keyOutput, valueOutput);
		}
	}
}