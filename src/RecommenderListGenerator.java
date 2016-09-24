import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommenderListGenerator {
	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		public Map<Integer, List<Integer>> userWatchHistory = new HashMap<>();

		@Override
		protected void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration(); // -->/stopWords/stopWords.txt
			String filePath = conf.get("watchHistory");

			Path pt = new Path(filePath);// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();

			while (line != null) {
				int user = Integer.parseInt(line.split("\t")[0]);
				String[] movies_rating = line.split("\t")[1].split(",");
				List<Integer> movie_history = new ArrayList<>();
				for (String cur : movies_rating) {
					int movie = Integer.parseInt(cur.split(":")[0]);
					movie_history.add(movie);
				}
				userWatchHistory.put(user, movie_history);
				line = br.readLine();
			}
			br.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			int user = Integer.parseInt(tokens[0]);
			int movie = Integer.parseInt(tokens[1].split(":")[0]);
			double rating = Double.parseDouble(tokens[1].split(":")[1]);

			if (!userWatchHistory.get(user).contains(movie)) {
				context.write(new IntWritable(user), new Text(movie + ":" + rating));
			}
		}
	}

	public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		public Map<Integer, String> movieTitles = new HashMap<>();

		@Override
		protected void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration(); // -->/stopWords/stopWords.txt
			String filePath = conf.get("movieTitles");

			Path pt = new Path(filePath);// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();

			while (line != null) {
				int movie_id = Integer.parseInt(line.trim().split(",")[0]);
				String movie_name = line.trim().split(",")[1];
				movieTitles.put(movie_id, movie_name);
				line = br.readLine();
			}
			br.close();
		}

		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			while(values.iterator().hasNext()) {
				String cur = values.iterator().next().toString();
				int movie_id = Integer.parseInt(cur.split(":")[0]);
				String rating = cur.split(":")[1];
				context.write(key, new Text(movieTitles.get(movie_id) + ":" + rating));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("watchHistory", args[0]);
		conf.set("movieTitles", args[1]);

		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);

		job.setJarByClass(RecommenderListGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}
