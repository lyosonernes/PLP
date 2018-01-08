package countByType;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;


public class CountTreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String ligne = value.toString();
		if(ligne.contains("HAUTEUR"))
			return;
		else
		{
			String[] arbre = ligne.split(";");
			String type = arbre[2];
			context.write(new Text(type), one);
		}
	}
}






