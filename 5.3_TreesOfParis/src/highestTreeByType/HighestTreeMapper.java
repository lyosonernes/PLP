package highestTreeByType;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;


public class HighestTreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		try
		{
			String ligne = value.toString();
			if(ligne.contains("HAUTEUR"))
				return;
			else
				{
				String[] arbre = ligne.split(";");
				String type = arbre[2];
				double temp_height = Double.parseDouble(arbre[6]);
				int height = (int) temp_height;
				context.write(new Text(type), new IntWritable(height));
				}
		}
		catch (Exception exception)
		{
			exception.printStackTrace();
		}
	}
}






