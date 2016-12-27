package ufl.cloudcomp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DMap extends Mapper<Object, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	String line = value.toString();
	
	//Tokenize each line of input on white spaces as well as standard punctuation marks
	StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]'");

	while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		context.write(word, one); //emit a key value of <<word>, 1>
	
		}
	}
}