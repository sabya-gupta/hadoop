package edur.assign.three;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class WordCountMain{

	public static class Map extends WordCountMap{
		
	}
	
	public static class Reduce extends WordCountReduce{
		
	}
	
	public static void main(String[] arg) {
		try{
			HadoopUtil.runJob(WordCountMain.class, "myWordCount", Text.class, IntWritable.class, Map.class, Reduce.class, TextInputFormat.class, TextOutputFormat.class, arg[0], arg[1]);			
		}catch(IOException iox){
			iox.printStackTrace();
		}

	}

}
