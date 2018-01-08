package cs.bigdata.Lab2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cs.bigdata.Lab2.PageRankGraphMap;
import cs.bigdata.Lab2.PageRankGraphReduce;
import cs.bigdata.Lab2.PageRankPRMap;
import cs.bigdata.Lab2.PageRankPRReduce;
import cs.bigdata.Lab2.PageRankOrderMap;

public class PageRank extends Configured implements Tool {

	// Données du problèmes
	
	public static Double damp = 0.85;
	// on pourrai récuperer le nombre de noeuds dans le fichier en l'analysant
	public static double nbnoeuds = 75879;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    

	
    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);

        }
        
        IN_PATH = args[0];
        OUT_PATH = args[1];

		//Suppression du fichier de sortie s'il existe déjà
        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(new Path(OUT_PATH))) {
            fs.delete(new Path(OUT_PATH), true);
        }
		
		//Execution du job1 ie initialisation du graphe
		boolean job1fini = job1(IN_PATH, OUT_PATH + "/iter0");
        if (!job1fini) {System.exit(1);}
		
        String inPath = null;;
        String lastOutPath = null;
        
        for (int i = 0; i < 5; i++) {
			int isup = i+1;
            inPath = OUT_PATH + "/iter" + i;
            lastOutPath = OUT_PATH + "/iter" + isup;
            boolean job2fini = job2(inPath, lastOutPath);
            if (!job2fini) {System.exit(1);}
		}	
		
        boolean job3fini = job3(lastOutPath, OUT_PATH + "/result");
        if (!job3fini) {System.exit(1);}
		
		return 0;


    }
	
    public boolean job1(String pathin, String pathout) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
         // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche
        // Ce premier job permettra de transformer les données en "Noeud \t pagerank \t VertexVoisin" pour initialiser la tache
		Job job = Job.getInstance(getConf());
        job.setJobName("Graph");
		
        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankGraphMap.class);
        job.setReducerClass(PageRankGraphReduce.class);


        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        FileInputFormat.setInputPaths(job, new Path(pathin));
        FileOutputFormat.setOutputPath(job, new Path(pathout));



        return job.waitForCompletion(true);
     
}	

    public boolean job2(String pathin, String pathout) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche
        // Ce job permettra le calcul du pagerank, inspiré de "Design Patterns for Efficient Graph Algorithms in
//MapReduce"
		Job job = Job.getInstance(getConf());
        job.setJobName("PR");
		
        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankPRMap.class);
        job.setReducerClass(PageRankPRReduce.class);


        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        FileInputFormat.setInputPaths(job, new Path(pathin));
        FileOutputFormat.setOutputPath(job, new Path(pathout));



        return job.waitForCompletion(true);
     
}	

    public boolean job3(String pathin, String pathout) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche
        // Ce job permettra simplement d'organiser par ordre de pagerank
		Job job = Job.getInstance(getConf());
        job.setJobName("Order");
		
        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankOrderMap.class);

        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);


        // Définition des fichiers d'entrée et de sorties (ici considérés comme des arguments à préciser lors de l'exécution)
        FileInputFormat.setInputPaths(job, new Path(pathin));
        FileOutputFormat.setOutputPath(job, new Path(pathout));



        return job.waitForCompletion(true);
     
}

    public static void main(String[] args) throws Exception {

        PageRank exempleDriver = new PageRank();

        int res = ToolRunner.run(exempleDriver, args);

        System.exit(res);

    }

}

