import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day23App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Banking Analytics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val accounts = Seq(
      (1,"Amit","Savings"),
      (2,"Sara","Current"),
      (3,"Rahul","Savings"),
      (4,"Neha","Current")
    ).toDF("account_id","customer","account_type")

    val transactions = Seq(
      (1,1,"Credit",50000),
      (2,1,"Debit",10000),
      (3,1,"Debit",5000),
      (4,2,"Credit",70000),
      (5,2,"Debit",20000),
      (6,3,"Credit",40000),
      (7,3,"Debit",15000),
      (8,4,"Credit",90000),
      (9,4,"Debit",10000)
    ).toDF("transaction_id","account_id","type","amount")

    println("===== BANKING ANALYTICS =====")

    val data = accounts.join(transactions, Seq("account_id"))

    println("TRANSACTIONS:")
    data.show(false)

    println("ACCOUNT SUMMARY:")

    data.groupBy("account_id","customer")
      .agg(
        sum(
          when(col("type") === "Credit", col("amount"))
            .otherwise(0)
        ).as("total_credit"),

        sum(
          when(col("type") === "Debit", col("amount"))
            .otherwise(0)
        ).as("total_debit")
      )
      .withColumn(
        "balance",
        col("total_credit") - col("total_debit")
      )
      .show(false)

    println("TRANSACTION RANK:")

    val w = Window
      .partitionBy("account_id")
      .orderBy(desc("amount"))

    data.withColumn("rank", row_number().over(w))
      .show(false)

    println("HIGH VALUE TRANSACTIONS:")

    data.filter(col("amount") > 40000)
      .show(false)

    spark.stop()
  }
}
