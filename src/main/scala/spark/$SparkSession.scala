package spark

import org.apache.spark.sql.functions.{rand, rint}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Test

object $SparkSession {

  @Test
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("test").master("local[4]").getOrCreate()
    val df  = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep","\\t")
      .load("datas/data.csv")
      .toDF

    df.createTempView("tmp")

    spark.sql(
      s"""
        |select
        |     t1.id,
        |     t1.date_of_birth,
        |     t2.date_of_birth,
        |     t1.order_item,
        |     case
        |     when t1.date_of_birth='no value' then ${rint(rand*100000).cast("int").cast(StringType)}
        |     when length(t1.date_of_birth)=10  then t1.date_of_birth
        |     else 1 end as new
        |from tmp as t1
        |left join (
        |       select
        |           id,date_of_birth
        |       from tmp
        |       where date_of_birth != 'no value'
        |       ) as t2
        |on t1.id = t2.id;
        |""".stripMargin ).show()
/*    ss.sql(
      """
        |select
        | case id
        | when 1001 then "a"
        | when 1002 then "b"
        |from(
        |   select
        |       id
        |   from tmp
        |) t
        |""".stripMargin
    )*/
  }
}
