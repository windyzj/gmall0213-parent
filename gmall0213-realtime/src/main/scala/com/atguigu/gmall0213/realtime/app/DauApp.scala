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


    // 1  去重  把启动日志 只保留首次启动 （日活）
    //  redis去重
    // 去重  type:set  sadd     type:String setnx  既做了判断 又做了写入
    //set 当日的签到记录保存在一个set中   便于管理  整体查询速度快
    //string  把当天的签到记录 散列存储在redis   当量级巨大时 可以利用集群 做分布式
//    recordInputDstream.filter{record=>
//      val jsonString: String = record.value()
//       // 把json变成对象map jsonObject  case class
//
//      // 取出来 mid
//      //    用mid 保存一个清单 （set）
//      // redis  type ? set   key?  dau:2020-07-17   value ? mid
//      //  用sadd 执行
//      // 判断返回值 1或0  1 保留数据 0 过滤掉
//
//      null
//    }
    //得到一个过滤后的Dstream

    //写到es 中



      ssc.start()
      ssc.awaitTermination()
  }

}
