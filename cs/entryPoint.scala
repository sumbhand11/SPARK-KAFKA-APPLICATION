package edu.iu.soic.cs

import edu.iu.soic.cs.consume._
import edu.iu.soic.cs.produce._

class  entryPoint
object entryPoint {
     def main(args: Array[String]) {
       if (args.length < 1) {
         println("Help:")
         println("java -jar ./build/libs/kafka.spark-all.jar <arg0> <arg1> <arg2> <arg3> <arg4> <arg5>")
         System.exit(-1)
       
       } else if (args.length == 1) {
         if (args(0) == "produce") {
           println("********** DEBUG MODE : PRODUCER **********")
           ProducerEntryPoint.main(Array("13","topic1","hdfs://localhost:54310//user/ddos/input/test_input.large","localhost:9092","localhost:2181"))
         } else if (args(0) == "consume") {
           println("********** DEBUG MODE : CONSUMER **********")
           ConsumerEntryPoint.main(Array("topic1","localhost:2181","localhost:9092"))
         } else {
           println("ERROR: wrong argument(s)" + args.map { x =>println(x) })
           System.exit(-1)
         }
       } else {
         if (args(0) == "produce") {
           println("********** MAIN MODE : PRODUCER **********")
           ProducerEntryPoint.main(Array(args(1),args(2),args(3),args(4),args(5)))
         } else if (args(0) == "consume") {
           println("********** MAIN MODE : CONSUMER **********")
           ConsumerEntryPoint.main(Array(args(1),args(2),args(3)))
         } else {
           println("ERROR: wrong argument(s)" + args.map { x =>println(x) })
           System.exit(-1)
         }
       }
     }
}
