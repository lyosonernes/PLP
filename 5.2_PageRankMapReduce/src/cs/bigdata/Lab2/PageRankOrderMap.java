package cs.bigdata.Lab2;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// To complete according to your problem
public class PageRankOrderMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	// Overriding of the map method
	// On va inverser PageRank et Noeud clé dans :
	// Noeud clé	pagerank	Listedesnoeuds(sous le format : a,b,c,d,g,r).length
	// Cela permettra à Hadoop de trier par ordre décroissant
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
		
		int tabIndex1 = value.find("\t");
        int tabIndex2 = value.find("\t", tabIndex1 + 1);
        
        String page = Text.decode(value.getBytes(), 0, tabIndex1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), tabIndex1 + 1, tabIndex2 - (tabIndex1 + 1)));
        
		context.write(new DoubleWritable(pageRank), new Text(page));
    }
	
}






