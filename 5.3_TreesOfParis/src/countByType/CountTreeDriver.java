package countByType;

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


public abstract class CountTreeDriver extends Configured implements Tool 
{
    public static void main (String[] args) throws Exception 
    {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, " Nombre d'arbres par espèce");

        job.setJarByClass(CountTreeDriver.class);

        job.setMapperClass(CountTreeMapper.class);

        job.setReducerClass(CountTreeReducer.class);
        
       

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path("arbres.csv"));
                             
        FileOutputFormat.setOutputPath(job, new Path("CountByType"));

        FileSystem fs2 = FileSystem.newInstance(conf);

        if (fs2.exists(new Path("CountByType"))) 
        {
              fs2.delete(new Path("CountByType"), true);
        }

        if(job.waitForCompletion(true))
              System.exit(0);
        System.exit(1);
    }
}