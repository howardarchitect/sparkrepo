package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main1 {

	public static void main(String[] args) {

		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		
		List<Integer> input = new ArrayList<Integer>();
		input.add(1);
		input.add(2);	
		input.add(3);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> jrdd = sc.parallelize(input);
		
		long count = jrdd.count();
		Integer mapReduce = jrdd.map(a -> a*a).reduce((v1,v2) -> v1+v2);
		Integer reduce = jrdd.reduce((v1,v2) -> v1+v2);
		
		System.out.println(mapReduce);
		
		sc.close();
		
		
	}

}
