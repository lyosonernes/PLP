package cs.bigdata.TFIDF;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;



public class TFIDFReducer extends Reducer<Text,Text,Text,DoubleWritable> {
	// Reducer for getting the TfIDF
	// -Output key contains the word and docid seperated by a separator "--separator--"
	// -Output value is the Tf-IDF
	

	// Overriding of the reduce function
	public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

		int docsPerWord = 0;
		
		Iterator<Text> iteratorDocsPerWord = values.iterator();
		ArrayList<Text> cache = new ArrayList<Text>();
		
		// Getting the number of doc containing the given word
		while (iteratorDocsPerWord.hasNext()) {
			Text value = iteratorDocsPerWord.next();
			String valuesText = value.toString();
			String [] valuesTextArray = valuesText.split("--separator--");
			String wordCount = valuesTextArray[1];
			Text newValue = new Text();
			newValue.set(value);
			cache.add(newValue);
			if (Integer.parseInt(wordCount)>0) {
				docsPerWord++;
			}
		}
		int size = cache.size();
		// Computing the Tf-IDF
        for (int i = 0; i < size; ++i) {
			Text value = cache.get(i);
			String valuesText = value.toString();
			String [] valuesTextArray = valuesText.split("--separator--");
			Text keyOutput = new Text(key.toString() + "--separator--"+ valuesTextArray[0]);
			
			double wordCountInt = Integer.parseInt(valuesTextArray[1]);
			double wordsPerDocInt = Integer.parseInt(valuesTextArray[2]);
			DoubleWritable valueOutput = new DoubleWritable (((wordCountInt/wordsPerDocInt)*Math.log(2.0/docsPerWord)));

			context.write(keyOutput, valueOutput);
		}
	}
}