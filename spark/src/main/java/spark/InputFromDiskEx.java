package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class InputFromDiskEx {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFileRdd = sc.textFile("input.txt");
		
		textFileRdd
		  .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		  .collect()
		  .forEach(System.out::println);


		sc.close();
	}

}
