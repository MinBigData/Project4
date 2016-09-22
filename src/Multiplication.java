import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class Multiplication {
	public static class MultiplicationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();

		@Override
		protected void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration(); // -->/stopWords/stopWords.txt
			String filePath = conf.get("coOccurrencePath");

			Path pt = new Path(filePath);// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();

			while (line != null) {
				String[] tokens = line.toString().trim().split("\t");
				String[] movies = tokens[0].split(":");

				int movie1 = Integer.parseInt(movies[0]);
				int movie2 = Integer.parseInt(movies[1]);
				int relation = Integer.parseInt(tokens[1]);

				if (movieRelationMap.containsKey(movie1)) {
					MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
					movieRelationMap.get(movie1).add(movieRelation);
				} else {
					List<MovieRelation> list = new ArrayList<>();
					MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
					list.add(movieRelation);
					movieRelationMap.put(movie1, list);
				}
				line = br.readLine();
			}

			br.close();
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split("\t");
			String[] user_rating = tokens[1].split(":");

			int movie = Integer.parseInt(tokens[0]);
			int user = Integer.parseInt(user_rating[0]);
			double rating = Double.parseDouble(user_rating[1]);

			for (MovieRelation relation : movieRelationMap.get(movie)) {
				double score = rating * relation.getRelation();
				context.write(new IntWritable(user), new Text(relation.getMovie2() + ":" + score));
			}

		}
	}

	public static class MultiplicationReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<Integer, Double> recommendList = new HashMap<>();
			// values: movieid:score
			while (values.iterator().hasNext()) {
				String[] movie_score = values.iterator().next().toString().trim().split(":");
				int movie = Integer.parseInt(movie_score[0]);
				double score = Double.parseDouble(movie_score[1]);

				if (recommendList.containsKey(movie)) {
					double curScore = recommendList.get(movie);
					recommendList.put(movie, curScore + score);
				} else {
					recommendList.put(movie, score);
				}
			}

			Iterator iter = recommendList.keySet().iterator();
			while (iter.hasNext()) {
				int movie = (int) iter.next();
				double score = recommendList.get(movie);
				context.write(key, new Text(movie + ":" + score));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("coOccurrencePath", args[0]);

		Job job = Job.getInstance(conf);
		job.setMapperClass(MultiplicationMapper.class);
		job.setReducerClass(MultiplicationReducer.class);

		job.setJarByClass(Multiplication.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
