package com.marriott.reservation
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.marriott.reservation.kafkaConsumer.spark

object resMessageParser{
  
  def resParser(rawMessageDF: DataFrame) : DataFrame = {
    import spark.implicits._
    println("Deriving local revenue,create date key,property ID..")
    val creationDateTime = rawMessageDF.selectExpr("cast (reservation.creationDateTime as date)").distinct()
    val reservConfirmPropDF  = getReferenceData.getPropertyDetails(rawMessageDF)
    val accommSegmentAllDF = reservConfirmPropDF.alias("all")
                          .selectExpr("all.create_date","all.confo_num_orig","all.confo_num_curr","all.propertyCode","all.property_id","all.roomPools","explode(reservationSegments) as reserv").alias("in")
                          .selectExpr("in.*","explode(roomPools) as roomPoolsExp")
                          .selectExpr(
                              "create_date"
                              ,"confo_num_orig"
                              ,"confo_num_curr"
                              ,"propertyCode"
                              ,"property_id"
                              ,"roomPoolsExp.roomPoolCode"
                              ,"reserv.segmentStatus"
                              ,"reserv.startDate"
                              ,"reserv.endDate"
                              ,"reserv.rate.ratePlanCode"
                              ,"reserv.rate.baseRateUnit"
                              ,"reserv.rate.baseAmount"
                              ,"reserv.rate.currencyCode as currency_code"
                              ,"reserv.rate.decimalPlaces"
                            )
   val dimdate       = getReferenceData.getDateKey(creationDateTime)
   val dimArrivalDateKey = getReferenceData.getArrivalDateKey(accommSegmentAllDF)
   println("Running Data quality Checks...")
   val dataQualityErrors = validateAttributes.doChecks(accommSegmentAllDF)
   val currencyRate  = getReferenceData.getCurrencyRate(dimArrivalDateKey,dimdate)
   val reservationDF = currencyRate.alias("all")
                              .join(dataQualityErrors.alias("dq.*"),
                                  dataQualityErrors("confo_num_orig") ===currencyRate("confo_num_orig")
                                  and dataQualityErrors("confo_num_curr") ===currencyRate("confo_num_curr")
                                  and dataQualityErrors("create_date") ===currencyRate("create_date"))
                               .select("all.create_date","all.confo_num_orig","all.confo_num_curr","all.date_create_key","all.baseAmountLocal","all.baseAmount"
                                       ,"all.date_arrival_key","errorDescription")                           
    val reservationDFFinal = reservConfirmPropDF.alias("all")
                              .join(reservationDF.alias("ag"),
                                  reservConfirmPropDF("confo_num_orig") ===reservationDF("confo_num_orig")
                                  and reservConfirmPropDF("confo_num_curr") ===reservationDF("confo_num_curr")
                                  and reservConfirmPropDF("create_date") ===reservationDF("create_date"))
                                  .select("all.*","ag.date_create_key","ag.baseAmountLocal","ag.baseAmount","ag.date_arrival_key","ag.errorDescription")  
                  //  reservationDFFinal.show             
   reservationDFFinal 
}
}
