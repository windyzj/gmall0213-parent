package com.atguigu.gmall0213.realtime.ads

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object TrademarkStatApp {


    def main(args: Array[String]): Unit = {

      // 加载流 //手动偏移量
      val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ads_trademark_stat")
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      val groupId = "ads_trademark_stat_group"
      val topic = "DWS_ORDER_WIDE";


      //1   从Mysql中读取偏移量   （启动执行一次）
      val offsetMapForKafka: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic, groupId)

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


      // 1 提取数据
      val orderWideDstream: DStream[OrderWide] = inputGetOffsetDstream.map { record =>
        val jsonString: String = record.value()
        //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
        val orderWide: OrderWide = JSON.parseObject(jsonString, classOf[OrderWide])
        orderWide
      }
      // 聚合
      val trademarkAmountDstream: DStream[(String, Double)] = orderWideDstream.map(orderWide => (orderWide.tm_id + "_" + orderWide.tm_name, orderWide.final_detail_amount))
      val tradermarkSumDstream: DStream[(String, Double)] = trademarkAmountDstream.reduceByKey(_ + _)

      tradermarkSumDstream.print(1000)

      ssc.start()
      ssc.awaitTermination()
    }


}
