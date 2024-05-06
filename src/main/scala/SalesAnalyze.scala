import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lit, round, row_number, unix_timestamp}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object SalesAnalyze {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local[4]").getOrCreate()
    import session.implicits._

    val scheme = new StructType()
      .add(StructField("id", IntegerType, nullable = true))
      .add(StructField("product_id", IntegerType, nullable = true))
      .add(StructField("sales_date", StringType, nullable = true))
      .add(StructField("quantity", IntegerType, nullable = true))
      .add(StructField("price", FloatType, nullable = true))

    val pattern = "yyyy-MM-dd"

    val countries = session
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .schema(scheme)
      .csv("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/sales/sales.csv")

    val countriesWithTS = countries
      .withColumn("timestampCol", unix_timestamp(countries("sales_date"), pattern).cast("timestamp"))


    val productWindow = Window
      .partitionBy(countriesWithTS("product_id")).orderBy(countriesWithTS("timestampCol"))
      .rowsBetween(2, Window.currentRow)

    countriesWithTS
      .withColumn("moved_avg",
        round(avg(countriesWithTS("price")).over(productWindow), 2))
      .show()



  }

}
