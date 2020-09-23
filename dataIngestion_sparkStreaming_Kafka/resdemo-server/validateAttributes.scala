package com.marriott.reservation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, collect_set, when}
import com.marriott.reservation.kafkaConsumer.spark

object validateAttributes {
  
    import spark.implicits._

  def doChecks(accommSegmentAllDF: DataFrame): DataFrame = {
      val errorDescriptionDF = accommSegmentAllDF.select($"create_date",$"confo_num_orig",$"confo_num_curr",
        array(when($"roomPoolCode"==="",struct(lit("roomPoolCode").alias("errorField") ,lit("Column is NULL").alias("errorDesc"))),
        when($"property_id".isNull,struct(lit("property_id").alias("errorField") ,lit("Property does not exisits").alias("errorDesc"))))
      .alias("errorDescription"))
      .groupBy("create_date", "confo_num_orig", "confo_num_curr")
      .agg(collect_set("errorDescription").alias("errorDescription"))
    errorDescriptionDF
    
  }
}