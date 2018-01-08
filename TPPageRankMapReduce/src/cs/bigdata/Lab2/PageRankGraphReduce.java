package cs.bigdata.Lab2;
import cs.bigdata.Lab2.PageRank;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// To complete according to your problem

public class PageRankGraphReduce extends Reducer<Text, Text, Text, Text> {

    // Overriding of the reduce function
	// A chaque noeud on va associé la liste de ses voisins ainsi qu'un PR initial

    @Override

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {

	
		boolean first = true;
        String list = (1 / PageRank.nbnoeuds) + "\t";

        for (Text value : values) {
            if (!first) 
                list += ",";
            list += value.toString();
            first = false;
        }

		context.write(key, new Text(list));

    }

}

