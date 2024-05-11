package Music1;

import java.io.IOException;

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

public class Music1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		@SuppressWarnings("deprecation")
		Job j = new Job(c, "Music1");
		
		j.setJarByClass(Music1.class);
		j.setMapperClass(MusicMapper.class);
		j.setReducerClass(MusicReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(j, output);
		FileInputFormat.addInputPath(j, input);
		
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MusicMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
			
			String line = values.toString();
			String[] parts = line.split("\t");
			
			if(parts.length >= 5){
			Text songID = new Text(parts[1]);
			IntWritable shared = new IntWritable(Integer.parseInt(parts[2]));
			
			context.write(songID, shared);
			}
		}
	}
	
	public static class MusicReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			int count = 0;
			int sharedcount = 0;
			
			for(IntWritable value : values){
				++count;
				
				sharedcount += value.get();
			}
			
			String uniqueListener = "No. of Unique Listener for " + key.toString() + " : ";
			String shares = "No of times " + key.toString() + " was shared : ";
			
			context.write(new Text(uniqueListener), new IntWritable(count));
			context.write(new Text(shares), new IntWritable(sharedcount));
		}
	}
	
	

}
