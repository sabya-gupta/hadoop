package edur.assign.three;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TempMaxMinMain {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report)throws IOException {
			HadoopUtil.mapMaxMinTemp(key, value, output, report);
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		public void reduce(Text year, Iterator<Text> temps, OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
			HadoopUtil.reduceMaxMinTemp(year, temps, output, arg3);
		}
	}
	
	public static void main(String[] arg) {
		try{
			HadoopUtil.runJob(WordCountMain.class, "myMaxMinTemp", Text.class, Text.class, Map.class, Reduce.class, TextInputFormat.class, TextOutputFormat.class, arg[0], arg[1]);			
		}catch(IOException iox){
			iox.printStackTrace();
		}

	}

}
