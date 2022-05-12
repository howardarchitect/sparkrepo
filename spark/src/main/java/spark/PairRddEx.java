package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class PairRddEx {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> baseRdd = sc.parallelize(inputData);
		
 
		baseRdd
		.mapToPair(rowValue -> new Tuple2<String,Long>(rowValue.split(":")[0],1L))
		.reduceByKey((v1,v2) -> v1+v2)
		.foreach(t -> System.out.println(t._1()+" level has count of  ==   "+t._2()));
		
		// groupby
		
		baseRdd
		.mapToPair(rowValue -> new Tuple2<String,Long>(rowValue.split(":")[0],1L))
		.groupByKey()
		.foreach(t -> System.out.println(t._1()+
				" level has count of  ==   "+Iterables.size(t._2)));
		
		
		sc.close();

	}

}
