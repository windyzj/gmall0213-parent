package com.atguigu.gmall0213.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDbMaxwellApp {

  def main(args: Array[String]): Unit = {
    //消费kafka
    //
    //  偏移量管理？    精确一次消费？ kafka作为最后存储端 无法保证幂等性 只能做“至少一次消费”
    //  手动后置偏移量必须保证  防止宕机丢失数据

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("base_db_maxwell_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "base_db_maxwell_group"
    val topic = "GMALL0213_DB_M";


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
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据 2 分topic 
    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    jsonObjDstream.foreachRDD{rdd=>

      rdd.foreach { jsonObj =>
        //解析json
        val tableName: String = jsonObj.getString("table")
        val optType: String = jsonObj.getString("type")
        val topicName = "ODS_" + tableName.toUpperCase
        // val dataArr: JSONArray = jsonObj.getJSONArray("data")
        val json: String = jsonObj.getString("data")
        //        println(s"tableName = ${tableName}")
        //        println(s"optType = ${optType}")
        if (json != null && json.length > 3) {
          if ((tableName.equals("order_info") && optType.equals("insert"))
            || (tableName.equals("order_detail") && optType.equals("insert"))
            || (tableName.equals("base_province"))
            || (tableName.equals("user_info"))
            || (tableName.equals("sku_info"))
            || (tableName.equals("base_trademark"))
            || (tableName.equals("base_category3"))
            || (tableName.equals("spu_info"))
          ) {
            //发送到kafka主题
            MyKafkaSink.send(topicName, json);
            Thread.sleep(400)
          }
        }
      }
      //driver 提交偏移量
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }



}
