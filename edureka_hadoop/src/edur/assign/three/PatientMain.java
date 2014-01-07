package edur.assign.three;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PatientMain {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)throws IOException {
			HadoopUtil.mapPatient(key, value, output, report);
		}
		
	}
	
	public static class Reduce extends WordCountReduce{
		
	}
	
	public static void main(String[] arg) {
		try{
			HadoopUtil.runJob(WordCountMain.class, "myPatientCount", Text.class, IntWritable.class, Map.class, Reduce.class, TextInputFormat.class, TextOutputFormat.class, arg[0], arg[1]);			
		}catch(IOException iox){
			iox.printStackTrace();
		}

	}

}
