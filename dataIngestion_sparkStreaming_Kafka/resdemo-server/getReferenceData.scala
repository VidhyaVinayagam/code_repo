package com.marriott.reservation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum
import com.marriott.reservation.kafkaConsumer.spark

trait createRefDataFrame {
        val df : DataFrame =  spark.sql("select date_key,to_date(date_dt) as date_dt,year_acctg from lz_mcom_mstr_dbo.mrdw_dim_date")
          df.createOrReplaceTempView("mrdw_dim_date")
      df.cache()
}

object getReferenceData extends createRefDataFrame{
  
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
      def getDateKey(indateDF: DataFrame): DataFrame = {
      val dimDate = spark.sql("select * from mrdw_dim_date")
      val dimDateCurrent = dimDate.join(indateDF, $"date_dt" === $"creationDateTime")
        .select($"date_key".as("date_create_key"), $"creationDateTime".as("creation_date"), $"year_acctg")
      dimDateCurrent
    }
  
   def getArrivalDateKey(accommSegmentAllDF: DataFrame): DataFrame = {
    val dimDate = spark.sql("select * from mrdw_dim_date")
    val dimDateArrival = accommSegmentAllDF.alias("all").join(dimDate, $"date_dt" === $"startDate")
      .select($"all.*",$"date_key".as("date_arrival_key"))
    dimDateArrival
  }
      def getPropertyDetails(inDataDF: DataFrame): DataFrame = {
      val dimProperty = spark.sql("select property_id,property_cd from lz_mcom_mstr_dbo.mrdw_dim_property")      
      dimProperty.cache()
      val dimPropertyDetail = inDataDF.alias("all").join(dimProperty, $"property.propertyCode" === $"property_cd", "left_outer")
        .selectExpr("all.*","cast(reservation.creationDateTime as date) as create_date"
                            ,"reservation.reservationConfirmations.reservationCode[0] as confo_num_orig"
                            ,"nvl(reservation.reservationConfirmations.reservationCode[1],reservation.reservationConfirmations.reservationCode[0]) as confo_num_curr"
                            ,"reservation.reservationConfirmations.reservationInstance[0] as reservationInstance"
                            ,"property.propertyCode as propertyCode","property_id")   
      dimPropertyDetail
    }
      
      import org.apache.spark.sql.functions._

    def getCurrencyRate(resCurrencyCode: DataFrame, dimDate: DataFrame): DataFrame = {
    val dimCurrencyConv = spark.sql("select currency_iso_cd,date_currency_key,date_key,exch_per_us_dlr_rte from lz_mcom_mstr_dbo.mrdw_dim_currency_conversion")
    val dimCurrency     = spark.sql("select currency_iso_cd,decimal_positions_marsha_qty from aw_mrdw_tgt_dbo.mrdw_dim_currency")
    dimCurrency.cache()
    dimCurrencyConv.cache()
      val dimCurrencyJoin = dimCurrencyConv.join(dimDate, $"date_create_key" === $"date_key").join(dimCurrency, "currency_iso_cd")
      val dimCurrencyDetails = resCurrencyCode.alias("all").join(dimCurrencyJoin, $"creation_date" === $"create_date" and $"currency_iso_cd" === $"currency_code")
        .selectExpr("all.*","create_date", "confo_num_orig", "confo_num_curr", "date_currency_key", "date_create_key", "exch_per_us_dlr_rte", "decimal_positions_marsha_qty").alias("in")
        .selectExpr("in.*", "baseAmount / pow(10,nvl(decimalPlaces,decimal_positions_marsha_qty)) * exch_per_us_dlr_rte  as baseAmountLocal")
        .groupBy("create_date", "confo_num_orig", "confo_num_curr", "date_create_key")
        .agg(sum("baseAmountLocal").as("baseAmountLocal"), sum("baseAmount").as("baseAmount"),collect_list("date_arrival_key").as("date_arrival_key"))
      dimCurrencyDetails
    }
}