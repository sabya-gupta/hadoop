package edur.assign.three;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)throws IOException {
		Text word = new Text();
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			output.collect(word, one);
		}
	}

}
