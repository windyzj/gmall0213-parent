package com.atguigu.gmall0213.realtime.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderDetailApp {

  def main(args: Array[String]): Unit = {

    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_detail_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dwd_order_detail_group"
    val topic = "ODS_ORDER_DETAIL";


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


    // 1 提取数据 2 分topic
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    orderDetailDstream.print(1000)

    //////////////////////////
    /////////维度关联： spu id name , trademark id name , category3 id name/////////
    //////////////////////////

    orderDetailDstream.foreachRDD{rdd=>
      rdd.foreach{orderDetail=>
        MyKafkaSink.send("DWD_ORDER_DETAIL",  JSON.toJSONString(orderDetail,new SerializeConfig(true)))

      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }


    ssc.start()
    ssc.awaitTermination()
  }





}
