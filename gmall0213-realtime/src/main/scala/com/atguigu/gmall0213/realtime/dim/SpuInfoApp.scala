package com.atguigu.gmall0213.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.SpuInfo
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object SpuInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_spu_info_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_SPU_INFO";
    val groupId = "dim_spu_info_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
      //startInputDstream.map(_.value).print(1000)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val objectDstream: DStream[SpuInfo] = inputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val obj: SpuInfo = JSON.parseObject(jsonStr, classOf[SpuInfo])
      obj
    }

    objectDstream.foreachRDD{rdd=>

      rdd.saveToPhoenix("GMALL0213_SPU_INFO",Seq("ID", "SPU_NAME"  )
        ,new Configuration,Some("hdp1,hdp2,hdp3:2181"))

      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
