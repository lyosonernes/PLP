package cs.bigdata.Tree;


import java.io.*;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class DisplayYearHeightTree {

	public static void main(String[] args) throws IOException {
		String localSrc = Paths.get(System.getProperty("user.dir"), "data", "arbres.csv").toString();
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			// Delimiter of the csv file
			String splitBy = ";";
			// read line by line
			String line = br.readLine();
			line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				Tree.readLine(line, splitBy);
				// go to the next line
				line = br.readLine();
			}
			br.close();
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
