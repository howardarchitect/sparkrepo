package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ToupleEx {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		List<Integer> input = new ArrayList<Integer>();
		input.add(2);
		input.add(4);
		input.add(6);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> baseRdd = sc.parallelize(input);
		
		JavaRDD<Tuple2<Integer,Integer>> intSquareTuple = baseRdd.map(a -> new Tuple2<Integer,Integer>(a, a*a));
		
		intSquareTuple.foreach(t -> System.out.println(t._1()+"--"+t._2()));

		sc.close();

	}

}
