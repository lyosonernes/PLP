package highestTreeByType;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class HighestTreeReducer extends Reducer< Text, IntWritable, Text, IntWritable> 
{
	private IntWritable maxHeight = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException,InterruptedException
    {
    	Iterator<IntWritable> iterator = values.iterator();
    	int temp_maxHeight= 0;
    	while(iterator.hasNext())
    	{
    		temp_maxHeight = Math.max(temp_maxHeight, iterator.next().get());
    	}
    	maxHeight.set(temp_maxHeight);
    	context.write(key, maxHeight);
    }
}
