package com.marriott.reservation


trait CustomVariables {
  
  val hadoopUserName = "edusrdps"
  val sparkAppName = "resetl"
  val sparkMasterName = "local[*]"
  val sparkSQLWarhouseDir = "/apps/hive/warehouse"
  val corruptDataPath = "C:\\Users\\gkali561\\Documents\\1.Portfolio\\1.ResETL\\Code\\Code\\data\\corrupt_data_"
  val hiveMetastoreURI = "thrift://clsllzlnxd17.devdata.marriott.com:9083"
  
  //Kafka Custom Variables
  val bootstrapServers = "localhost:9092"
  val kafkaGroupID = "group2"
  val kafkaOffsetReset = "smallest"
  val kafkaTopicName = "resetl"
  val batchDuration = 2 // in seconds
  val kafkaPollTime = "10000" // in milliseconds
  val kafkaAutoCommitInterval = "1000"
}
