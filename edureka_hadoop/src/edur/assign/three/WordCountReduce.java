package edur.assign.three;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReduce  extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text word, Iterator<IntWritable> numOfWords, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
		int total=0;
		while(numOfWords.hasNext()){
			total += numOfWords.next().get();
		}
		output.collect(word, new IntWritable(total));
	}

}
