import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};

object AirportsZeroStops {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("airportszerostops")
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

    val schema2 = StructType(
	Array(StructField("Airline", StringType,true),
	StructField("AirlineID", IntegerType,true),
	StructField("SourceAirport", StringType,true),
	StructField("SourceAirportID", IntegerType,true),
	StructField("DestinationAirport", StringType,true),
	StructField("DestinationAirportID", IntegerType,true),
	StructField("CodeShare", StringType,true),
	StructField("Stops", IntegerType,true),
	StructField("Equipment", StringType,true)))

    val df2 = sqlContext.read.format("com.databricks.spark.csv").schema(schema2).load("routes.csv")

    df2.registerTempTable("routes")

    val result1 = sqlContext.sql("select a.Airline, a.AirlineId, b.Name, b.Alias,a.SourceAirport,a.SourceAirportID,a.DestinationAirport,a.DestinationAirportID, a.Stops from routes a, airlines b where a.AirlineID = b.Airline and a.Stops = 0")

    result1.coalesce(2).write.format("com.databricks.spark.csv").option("header", "true").save("AirportsZeroStopsz")
}
}