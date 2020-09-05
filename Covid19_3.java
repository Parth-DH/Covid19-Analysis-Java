
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			String val[] = s.split(",");
			if (val[0].contentEquals("date")) {
				return;
			}
			DoubleWritable new_cases = new DoubleWritable(Double.parseDouble(val[2]));
			Text country = new Text(val[1]);
			context.write(country, new_cases);
		}
	}

	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		Map<String, Double> map = new HashMap<String, Double>();

		@Override
		protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				URI[] files = context.getCacheFiles();
				Path pt = new Path(files[0].getPath());
				// System.out.println(file.getPath());
				FileSystem fs = FileSystem.get(context.getConfiguration());
				FSDataInputStream fsDataInputStream = fs.open(pt);
				BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
				try {
					String line = br.readLine();
					while (line != null) {
						String[] val = line.split(",");
						if (val[1].equals("location")) {
							line = br.readLine();
						} else if (val.length < 5) {
							line = br.readLine();
						} else {
							map.put(val[1], Double.parseDouble(val[4]));
							line = br.readLine();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					br.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			for (DoubleWritable val : values) {
				sum += val.get();
			}

			if (this.map.containsKey(key.toString())) {
				context.write(key, new DoubleWritable(sum * 1000000 / this.map.get(key.toString())));
			} else {
				context.write(key, new DoubleWritable(0d));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "total reported cases");
		job.setJarByClass(Covid19_3.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.addCacheFile(new Path(args[1]).toUri());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}