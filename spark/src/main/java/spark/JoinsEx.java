package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class JoinsEx {

	public static void main(String[] args) 
	{
		List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
		visits.add(new Tuple2<>(4, 18));
		visits.add(new Tuple2<>(6, 4));
		visits.add(new Tuple2<>(10, 9));
		
		
		List<Tuple2<Integer, String>> users = new ArrayList<>();
		users.add(new Tuple2<>(4, "a"));
		users.add(new Tuple2<>(6, "b"));
		users.add(new Tuple2<>(10, "c"));
		
		users.add(new Tuple2<>(1, "d"));
		users.add(new Tuple2<>(2, "e"));
		users.add(new Tuple2<>(3, "f"));
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<Integer, Integer> visitsRdd = sc.parallelizePairs(visits);
		JavaPairRDD<Integer, String> usersRdd = sc.parallelizePairs(users);
		JavaPairRDD<Integer, Tuple2<Integer, String>> join = visitsRdd.join(usersRdd);

		
		  join.collect().forEach(System.out::println);
		  
		  JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoin = visitsRdd.leftOuterJoin(usersRdd);
		  leftOuterJoin.collect().forEach(t-> System.out.println(t._2._2.or("none").toUpperCase()));
		
		  
		  JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoin = visitsRdd.rightOuterJoin(usersRdd);
		  rightOuterJoin.collect().forEach(t-> System.out.println(t._2._2));
		 
		  
		  visitsRdd.fullOuterJoin(usersRdd);
		  
		sc.close();
	}

}
