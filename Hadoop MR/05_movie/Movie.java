package Movie;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Movie {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration c = new Configuration();
		String files[] = new GenericOptionsParser(c,args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "Movie");
		
		j.setJarByClass(Movie.class);
		j.setMapperClass(MovieMapper.class);
		j.setReducerClass(MovieReducer.class);
		j.setOutputValueClass(DoubleWritable.class);
		j.setOutputKeyClass(Text.class);
		
		FileOutputFormat.setOutputPath(j, output);
		FileInputFormat.addInputPath(j, input);
		
		System.exit(j.waitForCompletion(true)?0:1);
		
	}
	
	public static class MovieMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		public void map(LongWritable key, Text values, Context context ) throws IOException, InterruptedException{
			
			String line = values.toString();
			String parts[] = line.split("\t");
			
			if( parts.length >=4 ){
				Double rating = Double.parseDouble(parts[2]);
				String movieId = parts[1];
				
				context.write(new Text(movieId), new DoubleWritable(rating));
			}
					
		}
	}
	
	public static class MovieReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		private double globalMax = Double.MIN_VALUE;
		private List<String> movielist = new ArrayList<>();
		
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context){
			
			double max = Double.MIN_VALUE;
			
			for(DoubleWritable value : values){
				
				double val = value.get();
				if(val > max){
					max = val;
				}
			}
			if(max > globalMax){
				globalMax = max;
				movielist.clear();
				String movieid = key.toString();
				movielist.add(movieid);
			}else if(max == globalMax){
				movielist.add(key.toString());
			}

		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(String movie: movielist){
				context.write(new Text(movie), new DoubleWritable(globalMax));
			}
		}
	}

}
