package com.atguigu.gmall0213.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.OrderInfo
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderInfoApp {


  def main(args: Array[String]): Unit = {
      // 加载流 //手动偏移量
      val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_info_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dwd_order_info_group"
    val topic = "ODS_ORDER_INFO";


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
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      orderInfo
    }
    orderInfoDstream.map{orderInfo=>  //不够优化  //写入批次型  查询也可以 批次（周期+分区）   // 查询次数 (分批次)  查询数据内容（指定字段)
      val sql="select if_consumed from  user_state0213 where user_id="+orderInfo.user_id
      val list: List[JSONObject] = PhoenixUtil.queryList(sql)
      if(list!=null&&list.size>0){
          val jsonObj: JSONObject = list(0)
          val ifConsumed: String = jsonObj.getString("if_consumed")
           if(ifConsumed=="1"){   //只要用户没有消费过的标志 那么改变订单视为首充
             orderInfo.if_first_order="0"
           }else{
             orderInfo.if_first_order="1"
           }
      }else{
        orderInfo.if_first_order="1"//1?0?
      }

      orderInfo
    }
    //查询phoenix(hbase)  用什么？ user_id  查什么 if_consumed
    //  select if_consumed from  user_state0213 where user_id=xxxx
    //  用程序调用phoenix  jdbc ->sql 得到数据
    //  通过用户状态 给 订单打首充标志



    orderInfoDstream.print(1000)








    ssc.start()
    ssc.awaitTermination()

  }
}
