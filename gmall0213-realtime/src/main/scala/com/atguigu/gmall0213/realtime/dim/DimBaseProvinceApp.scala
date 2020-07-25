package com.atguigu.gmall0213.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.ProvinceInfo
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
object DimBaseProvinceApp {

  def main(args: Array[String]): Unit = {
    //1 从ods层(kafka) 获得对应维表数据  //2 偏移量后置 幂等
    //2 数据转换 case class
    //3 保存到hbase(phoenix)
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_base_province_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dim_base_province_group"
    val topic = "ODS_BASE_PROVINCE";


    //1   从redis中读取偏移量   （启动执行一次）
    val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafka != null && offsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var offsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputDstream.transform { rdd => //周期性在driver中执行
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val provinceDstream: DStream[ProvinceInfo] = inputGetOffsetDstream.map { record =>
      val json: String = record.value()
      val provinceInfo: ProvinceInfo = JSON.parseObject(json, classOf[ProvinceInfo])
      provinceInfo
    }


    //4  1 写入phoenix 2 提交偏移量
    provinceDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0213_PROVINCE_INFO",Seq("ID", "NAME",  "AREA_CODE","ISO_CODE","ISO_3166_2"),new Configuration,Some("hdp1,hdp2,hdp3:2181"))
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
