import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String gg = context.getConfiguration().get("world");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String s = value.toString();
			String val[] = s.split(",");
			if (val[0].contentEquals("date")) {
				return;
			}
			try {
				Date startDate = sdf.parse("2020-01-01");
				Date endDate = sdf.parse("2020-04-08");
				Date date = sdf.parse(val[0]);
				if (!(date.before(startDate) || date.after(endDate))) {
					if ((gg.equals("false") && !(val[1].equals("World"))) || gg.equals("true")) {
						IntWritable new_cases = new IntWritable(Integer.parseInt(val[2]));
						Text country = new Text(val[1]);
						context.write(country, new_cases);
					}
				}
			} catch (ParseException e) {
				e.getStackTrace();
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable reported_cases = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			reported_cases.set(sum);
			context.write(key, reported_cases);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("world", args[1]);
		Job job = Job.getInstance(conf, "total reported cases");
		job.setJarByClass(Covid19_1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}