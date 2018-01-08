package countByType;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CountTreeReducer extends Reducer< Text, IntWritable, Text, IntWritable> 
{
	private IntWritable TreeCount = new IntWritable();
    
	@Override
    protected void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException,InterruptedException
    {
        int sum = 0;     
    	Iterator<IntWritable> iterator = values.iterator();
    	while (iterator.hasNext()) 
    	{
    		sum += iterator.next().get();
    	}
    	TreeCount.set(sum);
    	context.write(key, TreeCount);
    }
}
