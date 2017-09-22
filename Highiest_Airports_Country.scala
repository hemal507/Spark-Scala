import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};

object MaxAirports {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("maxairports")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesRDD = sc.textFile(args(1))

    val schema =
      StructType(
        Array(StructField("AirportID", IntegerType, true),
          StructField("AirportName", StringType, true),
          StructField("City", StringType, true),
          StructField("Country", StringType, true),
          StructField("IATA", StringType, true),
          StructField("FAA", StringType, true),
          StructField("Lattitude", DoubleType, true),
          StructField("Longitude", DoubleType, true),
          StructField("Altitude", IntegerType, true),
          StructField("Timezone", DoubleType, true),
          StructField("DST", StringType, true),
          StructField("Tz", StringType, true)))

   val df = sqlContext.read.format("com.databricks.spark.csv").schema(schema).load("airports_mod.csv")

   df.registerTempTable("airports")

   sqlContext.sql("select Country, count(*) as cnt from airports group by Country order by cnt desc").limit(1).write.format("com.databricks.spark.csv").option("header", "true").save("c:\\spark\\airlines\\MaxAirports")
}
}

