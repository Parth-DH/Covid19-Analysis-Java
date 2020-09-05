import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.lang.String;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkCovid19_1 {

	static boolean filtering(String row, String start, String end) {
		String[] words = row.split(",");
		if (words[0].contentEquals("date")) {
			return false;
		} else {
			Date startDate;
			try {
				startDate = new SimpleDateFormat("yyyy-MM-dd").parse(start);
				Date endDate = new SimpleDateFormat("yyyy-MM-dd").parse(end);
				Date date = new SimpleDateFormat("yyyy-MM-dd").parse(words[0]);
				if (!(date.before(startDate) || date.after(endDate))) {
					return true;
				} else {
					return false;
				}
			} catch (ParseException e) {
				return false;
			}

		}
	}

	public static void main(String[] args) {
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

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("infected_Population_Counter");
		// Setting Master for running it from IDE.
		sparkConf.setMaster("local[1]");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		/* Reading input file whose path was specified as args[0] */
		JavaRDD<String> textFile = sparkContext.textFile(args[0]).filter(f -> filtering(f, args[1], args[2]));

		/*
		 * Below code generates Pair of Word with count as one similar to Mapper in
		 * Hadoop MapReduce
		 */
		JavaPairRDD<String, Integer> pairs = textFile.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] cols = t.split(",");
				return new Tuple2<String, Integer>(cols[1], new Integer(cols[3]));
			}
		});

		/*
		 * Below code aggregates Pairs of Same Words with count similar to Reducer in
		 * Hadoop MapReduce
		 */
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});

		/* Saving the result file to the location that we have specified as args[3] */
		counts.saveAsTextFile(args[3]);
		sparkContext.stop();
		sparkContext.close();
	}
}
