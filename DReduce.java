package ufl.cloudcomp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class DReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    private ArrayList<String> lookupWord = new ArrayList<String>();

    private Configuration conf;
    private BufferedReader fis;

	private IntWritable result = new IntWritable();


	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
	
	try{
	
	conf = context.getConfiguration();
        
	URI[] fileURIs = Job.getInstance(conf).getCacheFiles();
        
	for (URI fileURI : fileURIs) {
		
		Path filePath = new Path(fileURI.getPath());
		String fileName = filePath.getName().toString();
		
		//Parse the distributed cache file and store each word in an array list
		parseLookupFile(fileName);
        
		}

	}
	catch(NullPointerException e)
	{
		System.out.println("Exception : "+e);
	}
	
	}
	
	private void parseLookupFile(String fileName) {
		try {
			
			fis = new BufferedReader(new FileReader(fileName));
			String pattern = null;
        
			while ((pattern = fis.readLine()) != null) {
				
				//Tokenize the distributed file on white spaces and standard punctuation marks
				StringTokenizer tokens = new StringTokenizer(pattern, " \t\n\r\f,.:;?![]'");
					
				while(tokens.hasMoreTokens()) {
					
					//Adds each token to lookupWord
					lookupWord.add(tokens.nextToken());
				}
					
			}	
			
		} catch (IOException ioe) {
        
		System.err.println("Caught exception while parsing the cached file '"+ StringUtils.stringifyException(ioe));
      
	}
    
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		//If the key (word in the input file) present in the distributed cache file output its frequency 
		if (lookupWord.contains(key.toString())) {
			result.set(sum);
			context.write(key, result);
		}
	
	}

}
