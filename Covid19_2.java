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

public class Covid19_2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String startingDate = context.getConfiguration().get("start");
			String endingDate = context.getConfiguration().get("end");
			String s = value.toString();
			String val[] = s.split(",");
			if (val[0].contentEquals("date")) {
				return;
			}
			try {
				Date startDate = new SimpleDateFormat("yyyy-MM-dd").parse(startingDate);
				Date endDate = new SimpleDateFormat("yyyy-MM-dd").parse(endingDate);
				Date date = new SimpleDateFormat("yyyy-MM-dd").parse(val[0]);
				if (!(date.before(startDate) || date.after(endDate))) {
					IntWritable death_cases = new IntWritable(Integer.parseInt(val[3]));
					Text country = new Text(val[1]);
					context.write(country, death_cases);
				}
			} catch (ParseException e) {
				e.getStackTrace();
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable total_death_cases = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			total_death_cases.set(sum);
			context.write(key, total_death_cases);
		}
	}
	

	public static void main(String[] args) throws Exception {
		try {
			Date start_Date = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
			Date end_Date = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
			if (end_Date.before(start_Date)) {
				System.out.println("INVALID: End Date is before Start Date");
				System.exit(1);
			}
		} catch (ParseException e) {
			System.out.println("INVALID: Wrong Date Format");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		conf.set("start", args[1]);
		conf.set("end", args[2]);
		Job job = Job.getInstance(conf, "total number of deaths");
		job.setJarByClass(Covid19_2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}