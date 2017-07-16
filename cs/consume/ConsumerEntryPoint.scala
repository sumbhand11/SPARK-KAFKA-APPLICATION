package edu.iu.soic.cs.consume

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import kafka.serializer._
import kafka.utils._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.io._

// Consumer
class  ConsumerEntryPoint
object ConsumerEntryPoint {
   def main(args: Array[String]) {
     
      if(args.length > 0 && args(0)=="--help") {
          println("arguments[3]: subscriberTopicList  ZooKeeperServer  kafkaBroker")
           System.exit(0)
      }
      
      // get subscriberTopicList  ZooKeeperServer  kafkaBroker
      val topicsSet = args(0).split(",").toSet
      val zooKeeperServer = args(1)
      val kafkaBroker = args(2)
      
      val conf = new SparkConf().
      setAppName("spark-kafka-demo-app-2").
      setMaster("local[2]").
      set("spark.driver.allowMultipleContexts", "false")

      val sc  = SparkContext.getOrCreate(conf)

      // Read data streams from Kafka!
      val ssc = new StreamingContext(sc, Seconds(2))

      // Create direct kafka stream with brokers and topics
      val kafkaParams = Map[String, String](
          "metadata.broker.list"            -> kafkaBroker,
          "zookeeper.connect"               -> zooKeeperServer,
          "group.id"                        -> "group-1",
          "zookeeper.connection.timeout.ms" -> "1000")
          
      val kafkaInputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      
      val outDirName = "hdfs://localhost:54310//user/ddos/output"
      
      if (true) { // Currently working approach
        var checkmap = scala.collection.mutable.Map[String,String]()
        var ddosIP = scala.collection.mutable.Set[String]()
        kafkaInputDStream.foreachRDD { anRDD =>
          val x = anRDD.foreach { line => 
                  val msg = line._2
                  val msgList = msg.split(" ")
                  val keyIP = msgList(0).trim()
                  val valTS = msgList(3).trim().substring(1)
                  
                  // Main DDOS detect logic
                  if (checkmap.contains(keyIP)) {
                    // Simple Logic: two simultaneous hits from same source are dos attack
                    if (checkmap.get(keyIP).get == valTS) {
                      ddosIP += keyIP
                      
                      // Write to file blocker -  
                      // http://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
                      if (ddosIP.contains(keyIP)) {
                        println (keyIP)
                      }
                    } else {
                      checkmap.update(keyIP, valTS)
                    }
                  } else {
                    checkmap.put(keyIP,valTS)
                  }
                  
          } // for each
        }  // for each RDD  
        
     } else { // TODO: Fix write to HDFS
          var checkmap = scala.collection.mutable.Map[String,String]()
          var ddosIP = scala.collection.mutable.Set[String]()
          
          kafkaInputDStream.foreachRDD { anRDD =>
            val x1 = anRDD.map { line => (line._2.split(" ").map { e => e.trim() }.array(0), line._2.split(" ").map { e => e.trim() }.array(3).substring(1)) }
            x1.foreach(y => println(y._1,y._2))
          } // for each RDD  
          
      } // if condition
    
    ssc.checkpoint("checkpoint-consume")
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    
   }

}


