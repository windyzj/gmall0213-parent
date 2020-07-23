package com.atguigu.gmall0213.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.bean.DauInfo
import com.atguigu.gmall0213.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dau_group"
    val topic = "GMALL_START0213";
    ///手动偏移量//////
    //1   从redis中读取偏移量   （启动执行一次）
    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）

    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    //4   把偏移量结束点 写入到redis中（每批次行一次）

    //1   从redis中读取偏移量   （启动执行一次）
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]]=null
    if(offsetMapForKafka!=null&&offsetMapForKafka.size>0){  //根据是否能取到偏移量来决定如何加载kafka 流
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMapForKafka,groupId )
    }else{
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic,ssc, groupId )
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange]=null    //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd => //周期性在driver中执行
      println(1111)
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    println(22222)

    //////////////////////////////////////////////////////////
    ///去重///////////////////////////////////////////////////
    //      recordInputDstream.map(_.value()).print(1000)
    //redis  1 快 2 管理性 3 代码可以变化
    // 1  去重  把启动日志 只保留首次启动 （日活）
    //  redis去重
    // 去重  type:set  sadd     type:String setnx  既做了判断 又做了写入
    //set 当日的签到记录保存在一个set中   便于管理  整体查询速度快
    //string  把当天的签到记录 散列存储在redis   当量级巨大时 可以利用集群 做分布式
    //    recordInputDstream.filter{record=>
    //      val jsonString: String = record.value()
    //       // 把json变成对象map jsonObject  case class

    // redis  type ? set   key?  dau:2020-07-18   value ? mid ...
    //sadd    既做了判断 又做了写入 成功1 失败0
    //      // 取出来 mid
    //      //    用mid 保存一个清单 （set）
    //      //  用sadd 执行
    //      // 判断返回值 1或0  1 保留数据 0 过滤掉
    //      null
    //    }
    //得到一个过滤后的Dstream

    //写到es 中

    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      // 把json变成对象map jsonObject  case class
      val jsonObject: JSONObject = JSON.parseObject(jsonString)
      jsonObject
    }


    val jsonObjFilteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
      //由于iterator 只能迭代一次  所以提取到list中
      val beforeFilteredlist: List[JSONObject] = jsonObjItr.toList
      println("过滤前：" + beforeFilteredlist.size)
      val jsonObjList = new ListBuffer[JSONObject]

      val jedis: Jedis = RedisUtil.getJedisClient
      for (jsonObj <- beforeFilteredlist) {
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val ts: lang.Long = jsonObj.getLong("ts")
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
        //      // redis  type ? set   key?  dau:2020-07-18   value ? mid ...
        val key = "dau:" + dt
        val flag: lang.Long = jedis.sadd(key, mid)

        //      // 判断返回值 1或0  1 保留数据 0 过滤掉
        if (flag == 1L) {
          jsonObjList.append(jsonObj)
        }
      }
      jedis.close()
      println("过滤后：" + jsonObjList.size)

      jsonObjList.toIterator

    }
    println(33333)



    //jsonObjFilteredDstream.print(1000)

    //写入到ES中
    jsonObjFilteredDstream.foreachRDD{rdd=>

     // rdd.foreach(jsonObj=>println(jsonObj)) // 写入数据库的操作
      rdd.foreachPartition{jsonObjItr=>
        val jsonObjList: List[JSONObject] = jsonObjItr.toList
        val formattor = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val dauWithIdList: List[(DauInfo, String)] = jsonObjList.map { jsonObj =>

          val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")

          //获取日期 、小时、分钟
          val ts: lang.Long = jsonObj.getLong("ts")
          val dateTimeString: String = formattor.format(new Date(ts))
          val dateTimeArr: Array[String] = dateTimeString.split(" ")
          val dt: String = dateTimeArr(0)
          val time: String = dateTimeArr(1)
          val timeArr: Array[String] = time.split(":")
          val hr: String = timeArr(0)
          val mi: String = timeArr(1)

          val dauInfo = DauInfo(commonJsonObj.getString("mid"),
            commonJsonObj.getString("uid"),
            commonJsonObj.getString("ar"),
            commonJsonObj.getString("ch"),
            commonJsonObj.getString("vc"),
            dt, hr, mi, ts
          )
          (dauInfo, dauInfo.mid) //日活表（按天切分索引)
        }
        val today= new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkSave(dauWithIdList,"gmall_dau_info0213_"+today)

      }

      OffsetManager.saveOffset(topic,groupId,offsetRanges)// 要在driver中执行 周期性 每批执行一次

    }


    //    jsonObjDstream.filter{jsonObj=>
    //      //mid   时间
    //      val mid: String = jsonObj.getJSONObject("common").getString("mid")
    //      val ts: lang.Long = jsonObj.getLong("ts")
    //
    //      val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
    //      val jedis = new Jedis("hdp1",6379)
    //      // redis  type ? set   key?  dau:2020-07-18   value ? mid ...
    //      val key="dau:"+dt
    //      val flag: lang.Long = jedis.sadd(key,mid)
    //      jedis.close()
    //      // 判断返回值 1或0  1 保留数据 0 过滤掉
    //      if(flag==1L){
    //        true
    //      }else{
    //        false
    //      }
    //
    //    }


    ssc.start()
    ssc.awaitTermination()
  }

}
