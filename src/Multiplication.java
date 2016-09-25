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

public class Multiplication {
	public static class MultiplicationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		/*
		 * [movie1 {movie1, movie2, 8}{movie1, movie3, 5}{movie1, movie7, 6}]
		 * [movie2 {movie2, movie1, 8}{movie2, movie5, 9}{movie2, movie9, 10}]
		 * ....
		 */
		/*
		 * {user 	movie:rating}
		 */
		
		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<>();
		Map<Integer, Integer> denominator = new HashMap<>();

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
			
			for(Map.Entry<Integer, List<MovieRelation>> entry: movieRelationMap.entrySet()) {
				int sum = 0;
				for(MovieRelation relation: entry.getValue()) {
					sum += relation.getRelation();
				}
				denominator.put(entry.getKey(), sum);
			}
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().trim().split(",");

			int user = Integer.parseInt(tokens[0]);
			int movie = Integer.parseInt(tokens[1]);
			double rating = Double.parseDouble(tokens[2]);

			for (MovieRelation relation : movieRelationMap.get(movie)) {
				double score = rating * relation.getRelation() / denominator.get(relation.getMovie2());
				DecimalFormat df = new DecimalFormat("#.00");      
				score = Double.valueOf(df.format(score));
				context.write(new IntWritable(user), new Text(relation.getMovie2() + ":" + score));
			}

		}
	}

	public static class MultiplicationReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<Integer, Double> finalList = new HashMap<>();
			// values: movieid:score
			while (values.iterator().hasNext()) {
				String[] movie_score = values.iterator().next().toString().trim().split(":");
				int movie = Integer.parseInt(movie_score[0]);
				double score = Double.parseDouble(movie_score[1]);

				if (finalList.containsKey(movie)) {
					double curScore = finalList.get(movie);
					finalList.put(movie, curScore + score);
				} else {
					finalList.put(movie, score);
				}
			}

			Iterator iter = finalList.keySet().iterator();
			while (iter.hasNext()) {
				int movie = (int) iter.next();
				double score = finalList.get(movie);
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
