package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapReduceEx {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		List<Integer> input = new ArrayList<Integer>();
		input.add(2);
		input.add(4);
		input.add(6);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> baseRdd = sc.parallelize(input);

		Integer mapReduce = baseRdd.map(a -> a * a).reduce((v1, v2) -> v1 + v2);

		System.out.println("--- " + mapReduce);

		sc.close();

	}

}
