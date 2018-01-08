package cs.bigdata.Lab2;
import cs.bigdata.Lab2.PageRank;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// To complete according to your problem

public class PageRankPRReduce extends Reducer<Text, Text, Text, Text> {

    // Overriding of the reduce function
	// on doit traité :
		// Noeud clé	@Listedesnoeuds(sous le format : a,b,c,d,g,r)
		// Noeud clé	pagerank	Listedesnoeuds(sous le format : a,b,c,d,g,r).length
	
    @Override

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {

		String list = "";
        double sumPRenTplus1 = 0.0;
        
        for (Text value : values) {
 
            String a = value.toString();
            
            if (a.startsWith("@")) {
                list += a.substring("@".length());
            } else {
                
                String[] split = a.split("\\t");
                
                double pageRank = Double.parseDouble(split[0]);
                int nbVoisin = Integer.parseInt(split[1]);
				
                sumPRenTplus1 += (pageRank / nbVoisin);
            }

        }
        
		double PageRankTplus1 = 0.85 * sumPRenTplus1 + (1 - 0.85);
		context.write(key, new Text(PageRankTplus1 + "\t" + list));
		
    }

}

