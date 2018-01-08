package cs.bigdata.Lab2;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// To complete according to your problem
public class PageRankGraphMap extends Mapper<LongWritable, Text, Text, Text> {
// Overriding of the map method
@Override
protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
		
		// d'après l'analyse des données, le document contient les noeuds reliés séparés par un tab "\t".
		// On va supprimer les premières lignes pour faciliter la lecture du documents
		// on va sur ce map mapper les noeuds entre eux
		
		int tabIndex = value.find("\t");
        String noeud1 = Text.decode(value.getBytes(), 0, tabIndex);
        String noeud2 = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
		context.write(new Text(noeud1), new Text(noeud2));
	
		
        
    }
}






