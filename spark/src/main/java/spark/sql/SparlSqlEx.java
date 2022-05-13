package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparlSqlEx {

	public static void main(String[] args) {

		// System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder().appName("startingSparkSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:////C:/tmp/").getOrCreate();

		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("students.csv");
		
		Row firstRow = dataset.first();
		
		String subject = firstRow.get(2).toString();
		System.out.println(subject);
		dataset.show();
		//sparkSession.close();
	}

}
