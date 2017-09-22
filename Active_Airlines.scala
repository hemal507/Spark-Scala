import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};

object ActiveAirlines {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("activeairlines")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesRDD = sc.textFile(args(1))

    val schema = StructType(
    Array(StructField("Airline", IntegerType,true),
	StructField("Name", StringType,true),
	StructField("Alias", StringType,true),
	StructField("IATA", StringType,true),
	StructField("ICAO", StringType,true),
	StructField("Callsign", StringType,true),
	StructField("Country", StringType,true),
	StructField("Active", StringType,true)))

   val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).load("Final_airlines.csv")

   df.registerTempTable("airlines")

   sqlContext.sql("select  Airline, Name, Alias, Country , Active from airlines where Active = 'Y'").write.format("com.databricks.spark.csv").option("header", "true").save("ActiveAirlines")
}
}

