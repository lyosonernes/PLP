package cs.bigdata.ISDHistory;


import java.io.*;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;




public class DisplayISDHistory {

	public static void main(String[] args) throws IOException {
		String localSrc = Paths.get(System.getProperty("user.dir"), "data", "isd-history.txt").toString();
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			// read line by line
			String line = br.readLine();
			for (int i=0;i<22;i++) {
			line = br.readLine();
			}
			while (line !=null){
				System.out.println(line);
				Station.readLine(line);
				// Process of the current line
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
