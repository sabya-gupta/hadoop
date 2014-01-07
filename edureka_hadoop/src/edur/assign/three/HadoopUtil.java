package edur.assign.three;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HadoopUtil {

	public static void runJob(Class jobClass,  String jobName, Class outPutKeyClass, Class outputValueClass, Class mapClass, Class reduceClass, Class inputFormat, Class outputFormat,
			String inputPath, String outputPath) throws IOException{

		JobConf conf = new JobConf(jobClass);
		conf.setJobName(jobName);

		conf.setOutputKeyClass(outPutKeyClass);
		conf.setOutputValueClass(outputValueClass);

		conf.setMapperClass(mapClass);
		conf.setReducerClass(reduceClass);

		conf.setInputFormat(inputFormat);
		conf.setOutputFormat(outputFormat);

		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		JobClient.runJob(conf);

	}
	
	
	public static void mapPatient(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)throws IOException {
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String pID = "";
		int cntr=0;
		while(line.charAt(cntr)!=' '){
			pID=pID+line.charAt(cntr);
			cntr++;
		}
		output.collect(new Text(pID), one);
	}

	
	public static void mapTemp(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)throws IOException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		output.collect(new Text(st.nextToken()), new IntWritable(Integer.parseInt(st.nextToken())));
	}
	
	public static void reduceMaxTemp(Text year, Iterator<IntWritable> temps, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
		int maxTemp=-255;
		while(temps.hasNext()){
			int _temp = temps.next().get();
			if(_temp>maxTemp) maxTemp = _temp;
		}
		output.collect(year, new IntWritable(maxTemp));
	}
	
	public static void mapAlphabets(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter report)throws IOException {
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while (st.hasMoreTokens()){
			String _temp = st.nextToken();
			output.collect(new Text(_temp.length()+""), one);
		}
	}
	
	public static void mapMaxMinTemp(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report)throws IOException {
		String line = value.toString();
		String date = line.substring(6, 14);

		String strMaxTemp = line.substring(38, 45);
		float maxTemp = Float.parseFloat(strMaxTemp.trim());
		
		String strMinTemp = line.substring(46, 53);
		float minTemp = Float.parseFloat(strMinTemp.trim());
		
		if(maxTemp<100 && maxTemp>-100){
			output.collect(new Text("MAX_TEMP"), new Text(strMaxTemp));
			if(maxTemp>30) output.collect(new Text("HOT_DAY"), new Text(date));

		}
		if(minTemp<100 && minTemp>-100){
			output.collect(new Text("MIN_TEMP"), new Text(strMinTemp));
			if(minTemp<10) output.collect(new Text("COLD_DAY"), new Text(date));
		}

	
	}

	public static void reduceMaxMinTemp(Text key, Iterator<Text> temps, OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
		float maxTemp=-255;
		float minTemp= 255;
		
		String _key = key.toString();
		if(_key.equals("MAX_TEMP")){
			while(temps.hasNext()){
				float _tmpTemp = Float.parseFloat(temps.next().toString());
				if(_tmpTemp>maxTemp) maxTemp = _tmpTemp;
			}
			output.collect(new Text("MAXIMUM TEMP"), new Text(maxTemp+""));
			return;
		}
		
		if(_key.equals("MIN_TEMP")){
			while(temps.hasNext()){
				float _tmpTemp = Float.parseFloat(temps.next().toString());
				if(_tmpTemp<minTemp) minTemp = _tmpTemp;
			}
			output.collect(new Text("MINIMUM TEMP"), new Text(minTemp+""));
			return;
		}
		
		if(_key.equals("HOT_DAY") || _key.equals("COLD_DAY")){
			while(temps.hasNext()){
				if(_key.equals("HOT_DAY"))
				output.collect(temps.next(), new Text(key.toString()+" (TEMP > 30 degC.)"));
				else{
					output.collect(temps.next(), new Text(key.toString()+" (TEMP < 10 degC.)"));					
				}
			}
		}
				
	}
	
	
}
