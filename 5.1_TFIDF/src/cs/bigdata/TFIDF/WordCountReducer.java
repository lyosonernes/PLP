package cs.bigdata.TFIDF;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;



public class WordCountReducer extends Reducer<Text,IntWritable,Text,Text> {
	// Reducer for WordCount:
	// - Output key is a Text containing the word and the docid(filename) seperated by a separator "--separator--"
	// -Output value contains the wordcount saved as a Text
	
	private IntWritable totalWordCount = new IntWritable();

	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values,
			final Context context) throws IOException, InterruptedException {

		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();

		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}

		totalWordCount.set(sum);

		context.write(key, new Text(totalWordCount.toString()));
	}
}