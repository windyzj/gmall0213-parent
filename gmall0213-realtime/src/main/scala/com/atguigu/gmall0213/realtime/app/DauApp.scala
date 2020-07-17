package com.atguigu.gmall0213.realtime.app

import com.atguigu.gmall0213.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
      val ssc = new StreamingContext(sparkConf,Seconds(5))
      val groupId="dau_group"
      val recordInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("GMALL_START0213",ssc,groupId)

      recordInputDstream.map(_.value()).print(1000)

      ssc.start()
      ssc.awaitTermination()
  }

}
