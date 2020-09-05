import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.String;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SparkCovid19_2 {

	static boolean filtering(String row, int colIndex, String skipCol) {
		String[] words = row.split(",");
		if (words[colIndex].contentEquals(skipCol)) {
			return false;
		} else {
			return true;
		}

	}

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("infected_Population_Counter");
		// Setting Master for running it from IDE.
		sparkConf.setMaster("local[1]");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		/* Reading input file whose path was specified as args[0] */
		JavaRDD<String> covidFile = sparkContext.textFile(args[0]).filter(f -> filtering(f, 0, "date"));
		JavaRDD<String> populationFile = sparkContext.textFile(args[1]).filter(f -> filtering(f, 1, "location"));
		Map<String, Double> map = new HashMap<String, Double>();
		List<String> rows = populationFile.collect();
		for (int i = 0; i < rows.size(); i++) {
			String[] column = rows.get(i).split(",");
			if (column.length < 5) {
				continue;
			} else {
				map.put(column[1], Double.parseDouble(column[4]));
			}
		}
		Broadcast<Map<String, Double>> popmap = sparkContext.broadcast(map);

		/*
		 * Below code generates Pair of Word with count as one similar to Mapper in
		 * Hadoop MapReduce
		 */
		JavaPairRDD<String, Double> pairs = covidFile.mapToPair(new PairFunction<String, String, Double>() {

			@Override
			public Tuple2<String, Double> call(String t) throws Exception {
				String[] cols = t.split(",");

				if (popmap.getValue().containsKey(cols[1])) {
					return new Tuple2<String, Double>(cols[1],
							Double.parseDouble(cols[2]) * 1000000 / popmap.getValue().get(cols[1]));
				} else {
					return new Tuple2<String, Double>(cols[1], 0d);
				}

			}
		});

		/*
		 * Below code aggregates Pairs of Same Words with count similar to Reducer in
		 * Hadoop MapReduce
		 */
		JavaPairRDD<String, Double> counts = pairs.reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double a, Double b) {
				return a + b;
			}
		});

		/* Saving the result file to the location that we have specified as args[2] */
		counts.saveAsTextFile(args[2]);
		sparkContext.stop();
		sparkContext.close();
	}
}
