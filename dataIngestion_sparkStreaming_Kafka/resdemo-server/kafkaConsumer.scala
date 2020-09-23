package com.marriott.reservation

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.consumer._
import org.apache.spark.sql.SaveMode
import kafka.serializer.StringDecoder
import scala.concurrent._
import org.apache.log4j._
import org.apache.log4j.Level._
import java.util.Base64

//Customer Variables import
import com.marriott.reservation.CustomVariables

trait SparkContextCustom extends CustomVariables {

	val conf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMasterName)
	val ssc = new StreamingContext(conf, Seconds(batchDuration))
	System.setProperty("HADOOP_USER_NAME", hadoopUserName)
	val spark = SparkSession
			.builder()
			.appName(sparkAppName)
			.master("sparkMasterName")
			.config("spark.driver.allowMultipleContexts", "true")
			.config("spark.sql.warehouse.dir", sparkSQLWarhouseDir)
			.config("hive.metastore.uris", hiveMetastoreURI)
			.enableHiveSupport()
			.getOrCreate()
}

object KafkaConnectParam extends CustomVariables {

	val kafkaParams = Map[String, String](
  "bootstrap.servers"           -> bootstrapServers,
  "group.id"                    -> kafkaGroupID,
  "auto.commit.interval.ms"     -> kafkaAutoCommitInterval,
  "key.deserializer"            -> "org.apache.kafka.common.serialization.StringDeserializer",
  "value.deserializer"          -> "org.apache.kafka.common.serialization.StringDeserializer",
  "auto.offset.reset"           -> kafkaOffsetReset,
  "enable.auto.commit"          -> "false"
)
}

object kafkaConsumer extends SparkContextCustom {
  
 def main(args: Array[String]) = {
		import spark.implicits._
		spark.sparkContext.setLogLevel("ERROR")
    val log = LogManager.getRootLogger
    log.setLevel(Level.ERROR)
			val starttime = System.currentTimeMillis()
      // Read the raw message from file
		//	val messageStream = KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc,KafkaConnectParam.kafkaParams,List(kafkaTopicName).toSet)
			  println("====================================================================")
        println("Reading raw message started @ " + Calendar.getInstance().getTime)
        println("====================================================================")
			val rawMessageOrigDF = spark.read.json("file:///pmserv/mdw/dev/nzscripts/RES/reservation1.json")
				println("Total Number of raw messages in input : " + rawMessageOrigDF.count())
				if (rawMessageOrigDF.columns.contains("_corrupt_record")) {
				  val rawMessageCorrDF =  rawMessageOrigDF.select("_corrupt_record").where("_corrupt_record is not null")
				  val dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss")
				  rawMessageCorrDF.write.text(corruptDataPath+ dateFormat.format(Calendar.getInstance.getTime))
				  println("Total Number of corrupted messages in input : " + rawMessageCorrDF.count())
				 }
								  
				val rawMessageDF = if ((rawMessageOrigDF.columns.contains("_corrupt_record"))) 
				                    rawMessageOrigDF.drop("_corrupt_record").where("_corrupt_record is null")
				                    else 
				                    rawMessageOrigDF
				                    
				println("Number of valid raw messages in input : " + rawMessageDF.count())
		/*	messageStream.foreachRDD { resMessagerdd =>
			if (resMessagerdd.count > 1) { Thread.`yield`() 
			*/
        // Read the raw message from file
							  import spark.implicits._
							  println("Parsing Started")
								val Finalall = resMessageParser.resParser(rawMessageDF)
								println("Number of parsed messages in output : " + Finalall.count())
				        println("Message Parsing completed @ " +  Calendar.getInstance().getTime)
                val endtime2 = System.currentTimeMillis()
                val timetaken2 = (endtime2 - starttime) / 1000
        println("====================================================================")
        println("Total time taken to parse message - " + timetaken2 + " Seconds")
        println("====================================================================\n\n")
								
								println("Starting write into table....\n")
					//			Finalall.write.mode("Overwrite").format("orc").saveAsTable("it_res_sec_dbo.mrdw_reservation_repository_orc")								
						//		Finalall.write.mode("Overwrite").format("parquet").saveAsTable("lzc_dev.mrdw_reservation_repository_prq")
								Finalall.write.mode("Append").format("orc").save("hdfs://clsllzlnxd15.devdata.marriott.com:8020/apps/hive/warehouse/it_res_sec_dbo.db/mrdw_reservation_repository/")
					//	Finalall.write.mode("Append").format("parquet").save("hdfs://clsllzlnxd15.devdata.marriott.com:8020/apps/hive/warehouse/lzc_dev.db/mrdw_reservation_repository_prq/")
								Finalall.show
					//			Finalall.select("confo_num_orig","confo_num_curr","create_date","reservationInstance","property_id","propertyCode",
					//			    "date_create_key","date_arrival_key","reservationSegments.segmentStatus","roomPools.roomPoolCode","ratePlans.marketCode","baseAmount","baseAmountLocal","errorDescription").show()
					//	    Finalall.selectExpr("confo_num_orig","confo_num_curr","create_date","property_id","propertyCode","explode(errorDescription)").show
				 println("Message Parsing completed @ " +  Calendar.getInstance().getTime)
        val endtime = System.currentTimeMillis()
        val timetaken1 = (endtime - starttime) / 1000
        println("====================================================================")
        println("Total time taken to parse message - " + timetaken1 + " Seconds")
        println("====================================================================\n\n")
	}
}