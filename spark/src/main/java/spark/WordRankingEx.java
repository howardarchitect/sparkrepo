package spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordRankingEx {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> textFileRdd = sc.textFile("boringwords.txt");
		JavaRDD<String> notBoringRDD = textFileRdd.filter(word -> Util.isNotBoring(word));
		List<String> take = notBoringRDD.take(50);
        System.out.println(take);
		//textFileRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator()).collect().forEach(System.out::println);

		sc.close();
	}

}
