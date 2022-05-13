package spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparlSqlEx {

	private static final String MY_STUDENTS_VIEWS = "my_students_views";

	public static void main(String[] args) {

		// System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession sparkSession = SparkSession.builder().appName("startingSparkSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:////C:/tmp/").getOrCreate();

		Dataset<Row> dataset = sparkSession.read().option("header", true).csv("students.csv");
		
		//Dataset<Row> modenArt = dataset.filter("subject='Modern Art AND year >=2007'");
		//Dataset<Row> modenArt = dataset.filter(row -> row.getAs("subject").equals("Modern Art"));
		
		//Column subjectCol = dataset.col("subject");
		//Column yearCol = dataset.col("year");
		//Dataset<Row> modenArt = dataset.filter(subjectCol.equalTo("Modern Art").and(yearCol.geq(2005)));
		
		dataset.createOrReplaceTempView(MY_STUDENTS_VIEWS);
		
		Dataset<Row> modenArt = sparkSession.sql("select * from my_students_views where subject='French'");
		
		modenArt.show();
		//sparkSession.close();
	}

}
