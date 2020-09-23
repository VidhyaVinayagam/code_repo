package com.marriott.reservation

trait CustomVariables {
  
  val hadoopUserName = "edusrdps"
  val sparkAppName = "resetl"
  val sparkMasterName = "local[*]"
  val sparkSQLWarhouseDir = "/apps/hive/warehouse"
  val corruptDataPath = "file:///pmserv/mdw/dev/nzscripts/RES/corrupt_data_"
  val hiveMetastoreURI = "thrift://clsllzlnxd17.devdata.marriott.com:9083"
  
  //Kafka Custom Variables
  val bootstrapServers = "localhost:9092"
  val kafkaGroupID = "group2"
  val kafkaOffsetReset = "largest"
  val kafkaTopicName = "resetl"
  val batchDuration = 2 // in seconds
  val kafkaPollTime = "10000" // in milliseconds
  val kafkaAutoCommitInterval = "1000"
}
