import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object AverageTimeOfProcessPerMachine {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val scheme = new StructType()
      .add(StructField("machine_id", IntegerType, nullable = true))
      .add(StructField("process_id", IntegerType, nullable = true))
      .add(StructField("activity_type", StringType, nullable = true))
      .add(StructField("timestamp", DoubleType, nullable = true))

    val machines = spark
      .read
      .option("delimiter", ";")
      .option("header", "true")
      .csv("/Users/andrej/IdeaProjects/sparkSandbox/src/main/resources/average_time_machine/machines.csv")

    machines.as("m1")
      .join(machines.as("m2"), $"m1.machine_id" === $"m2.machine_id"
        && $"m1.process_id" === $"m2.process_id" && $"m2.timestamp" > $"m1.timestamp")
      .groupBy($"m1.machine_id").agg(avg($"m2.timestamp" - $"m1.timestamp").as("processing_time"))
      .show()

  }

}
