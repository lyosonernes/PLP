package highestTreeByType;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public abstract class HighestTreeDriver extends Configured implements Tool 
{
    public static void main (String[] args) throws Exception 
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, " Hauteur maximale par espèce");

        job.setJarByClass(HighestTreeDriver.class);

        job.setMapperClass(HighestTreeMapper.class);

        job.setReducerClass(HighestTreeReducer.class);
        

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        
        
        FileInputFormat.addInputPath(job, new Path("arbres.csv"));
                             
        FileOutputFormat.setOutputPath(job, new Path("HeightByType"));

        FileSystem fs2 = FileSystem.newInstance(conf);

        if (fs2.exists(new Path("HeightByType"))) 
        {
              fs2.delete(new Path("HeightByType"), true);
        }

        if(job.waitForCompletion(true))
              System.exit(0);
        System.exit(1);
    }
}