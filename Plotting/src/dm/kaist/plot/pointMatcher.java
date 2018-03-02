package dm.kaist.plot;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class pointMatcher {
	
	public static void main(String[] args) throws IOException
	{
		//The local path of input data set.
		String inputPath = "chameleon.ds";
		
		//The local path of labeledOutput directory.
		String labeledOutput = "labeledOutput";
		
		//The number of dimensions 
		int dim = 2;

		//The local path to write output file for plotting using R
		String outputPath = "R_output";
		
		File dir = new File(labeledOutput);
		File[] files = dir.listFiles();
		
		//Pid, label
		HashMap<Long, Integer> map = new HashMap<Long, Integer>();
		
		for(File file : files)
		{

			FileReader fr = new FileReader(file.getAbsolutePath());
			BufferedReader br = new BufferedReader(fr);

			String line = "";
			
			while((line = br.readLine()) != null)
			{
				String[] toks = line.split(" ");
				long pId = Long.parseLong(toks[0]);
				int label = Integer.parseInt(toks[1])+2;
					
				map.put(pId, label);
			}
			
			br.close();
			fr.close();
		}
		
		
		//Generate output for plotting
		//Coordinate matching
		
		FileWriter bw = new FileWriter(outputPath);
		BufferedWriter fw = new BufferedWriter(bw);
		
		FileReader fr = new FileReader(inputPath);
		BufferedReader br = new BufferedReader(fr);
		
		String line = "";
		
		while((line = br.readLine()) != null)
		{
			String[] toks = line.split(" ");
			
			long pId = Long.parseLong(toks[0]);
			
			if(map.get(pId) == null)
			{
				line += " "+ 1;
				bw.write(line+"\n");
			}else
			{
				line += " "+ map.get(pId);
				bw.write(line+"\n");
			}

			
		}
		
		br.close();
		fr.close();
		
		bw.close();
		fw.close();
	}
}
