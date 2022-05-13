package spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InputEx2 {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/").getOrCreate();

		
		Dataset<Row> dataset = spark.read().option("header", true).csv("biglog.txt");
		
		
		dataset.createOrReplaceTempView("logging_table");

		Dataset<Row> results = spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table");
		//Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level");

		
		results.createOrReplaceTempView("logging_table");
		Dataset<Row> results2 = spark.sql(
				"select level,month, count(1) as total from logging_table "
				+ "group by level, month order by 1,2,3");
		
		
		
		results2.show(100);

		//spark.close();
	}
}
