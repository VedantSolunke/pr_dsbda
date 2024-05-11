package LogFile;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LogFile {

	public static <setJarByClass> void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c,"LogFile");
		
		j.setJarByClass(LogFile.class);
		j.setMapperClass(LogMapper.class);
		j.setReducerClass(LogReducer.class);
		j.setOutputValueClass(IntWritable.class);
		j.setOutputKeyClass(Text.class);
		
		FileOutputFormat.setOutputPath(j, output);
		FileInputFormat.addInputPath(j, input);
		
		System.exit(j.waitForCompletion(true)?0:1);		
	}
	
	public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd-MM-yyyy HH:mm");
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String[] parts = line.split("\t");
			
			String user = parts[1];
			String loginStr = parts[5];
			String logoutStr = parts[7];
			
			try {
				
				Date loginDate = DATE_FORMAT.parse(loginStr);
				Date logoutDate = DATE_FORMAT.parse(logoutStr);
				
				long sessionDuration = logoutDate.getTime() - loginDate.getTime();
				int sessionDurationMin = (int) (sessionDuration / (1000 * 60));
				
				context.write(new Text(user), new IntWritable(sessionDurationMin));
				
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
//		private IntWritable maxDuration = new IntWritable();
		private List<String> userWithMaxDuration = new ArrayList<>();
		private int globalMax = Integer.MIN_VALUE;
		
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context){
			
			int max = Integer.MIN_VALUE;
			for(IntWritable value : values){
				int duration = value.get();
				if(duration > max){
					max = duration;
				}
			}
			if(max > globalMax){
				globalMax = max;
				userWithMaxDuration.clear();
				userWithMaxDuration.add(key.toString());
			}else if(max == globalMax){
				userWithMaxDuration.add(key.toString());
			}
//			maxDuration.set(max);
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(String user : userWithMaxDuration){
				context.write(new Text(user), new IntWritable(globalMax));
			}
		}
	}

}
