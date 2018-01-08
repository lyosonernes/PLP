package cs.bigdata.Lab2;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// To complete according to your problem
public class PageRankPRMap extends Mapper<LongWritable, Text, Text, Text> {
// Overriding of the map method
@Override

protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
    {
		// le premier job a renvoyé :
			//Noeud clé		pagerank	Listedesnoeuds(sous le format : a,b,c,d,g,r)
        // On va renvoyé deux élements, l'un qui va servir à itérer le calcul du pagerank
		// l'autre pour le calcul du pagerank
		// on aura donc :
			// Noeud clé	@Listedesnoeuds(sous le format : a,b,c,d,g,r)
			// Noeud clé	pagerank	Listedesnoeuds(sous le format : a,b,c,d,g,r).length
		int tabIndex1 = value.find("\t");
        int tabIndex2 = value.find("\t", tabIndex1 + 1);
        
        String noeudcle = Text.decode(value.getBytes(), 0, tabIndex1);
        String pageRank = Text.decode(value.getBytes(), tabIndex1 + 1, tabIndex2 - (tabIndex1 + 1));
        String list = Text.decode(value.getBytes(), tabIndex2 + 1, value.getLength() - (tabIndex2 + 1));
        
        String[] voisin = list.split(",");
        for (String noeud : voisin) { 
            Text pageRankactualise = new Text(pageRank + "\t" + voisin.length);
            context.write(new Text(noeud), pageRankactualise); 
        }
        
		context.write(new Text(noeudcle), new Text('@' + list));
    }
}






