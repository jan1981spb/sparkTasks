import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Beeline {

  //  Дана таблица tab
  //  (City, country, population)
  //  Нужно вывести по каждой стране топ 2.
  //
  //  Москва Россия 10
  //  Питер Россия 7
  //  Воронеж Россия 1
  //  Минск Беларусь 5
  //
  //  Ожидаемый результат
  //    Москва Россия 10
  //  Питер Россия 7
  //  Минск Беларусь 5

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().master("local").getOrCreate()
    import session.implicits._

    val scheme = new StructType()
      .add(StructField("city", StringType, nullable = true))
      .add(StructField("country", StringType, nullable = true))
      .add(StructField("population", IntegerType, nullable = true))

    val countries = session
      .read
      .option("delimiter", ";")
      .option("header", "true")
      .schema(scheme)
      .csv("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/countries/countries.csv")

    val windowF = Window.partitionBy("country").orderBy(countries("population").desc)
    val countriesWithRn = countries.select(countries("city"), countries("country"), countries("population"), row_number.over(windowF).as("rn"))
      countriesWithRn.select(countries("city"), countries("country"), countries("population")).filter(countriesWithRn("rn") < 3).show()

  }



}
