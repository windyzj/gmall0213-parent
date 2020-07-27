package com.atguigu.gmall0213.realtime.dwd

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0213.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall0213.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object OrderInfoApp {


  def main(args: Array[String]): Unit = {
    // 加载流 //手动偏移量
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_info_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "dwd_order_info_group"
    val topic = "ODS_ORDER_INFO";


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
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      orderInfo
    }


    //查询phoenix(hbase)  用什么？ user_id  查什么 if_consumed
    //  select if_consumed from  user_state0213 where user_id=xxxx
    //  用程序调用phoenix  jdbc ->sql 得到数据
    //  通过用户状态 给 订单打首充标志

    /*  orderInfoDstream.map{orderInfo=>  //不够优化  //写入批次型  查询也可以 批次（周期+分区）   // 查询次数 (分批次)  查询数据内容量（指定字段)
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
      }*/

    //map-> filter -> store
    // 按照周期+分区 组成大sql查询
    // select xxx from user_state0213 where user_id in (xxx,xxx,x,xxx,xx,xx)
    val orderInfoWithFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>

      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList != null && orderInfoList.size > 0) {
        val userIdList: List[Long] = orderInfoList.map(orderInfo => orderInfo.user_id)
        //1,2,3    in ('1','2','3')"
        val sql = "select  USER_ID,IF_CONSUMED from  USER_STATE0213 where USER_ID in ('" + userIdList.mkString("','") + "')"
        val ifConsumedList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // List=>list[(k,v)]=> map
        val ifConsumedMap: Map[String, String] = ifConsumedList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap
        for (orderInfo <- orderInfoList) {
          //for (jsonObj <- ifConsumedList) {}  //
          val ifConsumed: String = ifConsumedMap.getOrElse(orderInfo.user_id.toString, "0")
          if (ifConsumed == "1") { //消费过的用户
            orderInfo.if_first_order = "0" //不是首单
          } else {
            orderInfo.if_first_order = "1" //否则是首单
          }
        }
      }
      orderInfoList.toIterator
    }
    // 问题：
    //同一批次 同一个用户两次下单 如何解决 只保证第一笔订单为首单 其他订单不能为首单
    //矫正
    // 1  想办法让相同user_id的订单在一个分区中， 这样只要处理 mapPartition中的list就行了
    //--》 上游写入kafka 时 用userId 当分区键

    //2  groupbykey  按照某一个键值进行分组
    //   每组  取第一笔订单设为首单    非第一笔 设为 非首单   ，前提是：已经被全部设为首单
    val orderInfoGroupByUserIdDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo)).groupByKey()
    val orderInfoRealWithFirstFlagDstream: DStream[OrderInfo] = orderInfoGroupByUserIdDstream.flatMap { case (userId, orderInfoItr) =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if (orderList != null && orderList.size > 0) {
        val orderInfoAny: OrderInfo = orderList(0) // 随便取一笔订单 用于检验是否被打了首单标志
        //需要修正的两个条件 1 一个批次内做了2笔以上订单  2 其中有首单  //需要修正
        if (orderList.size >= 2 && orderInfoAny.if_first_order == "1") {
          // 排序
          val sortedList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
          for (i <- 1 to sortedList.size - 1) { //不是本批次第一笔订单  要还原成非首单
            val orderInfoNotFirstThisBatch: OrderInfo = sortedList(i)
            orderInfoNotFirstThisBatch.if_first_order = "0"
          }
          sortedList
        } else {
          orderList
        }
      } else {
        orderList
      }
    }




    //orderInfoRealWithFirstFlagDstream.print(1000)

   //////////////////////////
    //////关联维表 /////////
    /////////////////////

    // 查询维度表（省份)
    //map 每条数 /  mapPartition 每个分区  mapPartition针对每个分区的数据进行查询得到结果后在分发给每条数据
    //transform 把整个维度表查询出来 在分发给各个分区

    //查询维表 地区
    //driver //启动执行 只执行一次 //如果数据有可能发生变化 查询周期性查询
//    val sql="select id ,name,area_code ,iso_3166_2 from gmall0213_province_info"
//    val provinceList: List[JSONObject] = PhoenixUtil.queryList(sql)
//    val provinceBC: Broadcast[List[JSONObject]] = ssc.sparkContext.broadcast(provinceList)
   // transform  foreachRDD
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoRealWithFirstFlagDstream.transform { rdd =>
      //rdd外面 driver 中
      val sql = "select ID ,NAME,AREA_CODE ,ISO_3166_2 from GMALL0213_PROVINCE_INFO" //如果使用场景是整表查询 不用做预分区  不要加盐了
    val provinceList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val provinceMap: Map[lang.Long, JSONObject] = provinceList.map { jsonObj => (jsonObj.getLong("ID"), jsonObj) }.toMap

      val provinceMapBC: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(provinceMap)
      //rdd.xxx{ ex中}
      val rddWithProvince: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[lang.Long, JSONObject] = provinceMapBC.value
        val provinceJsonObj: JSONObject = provinceMap.getOrElse(orderInfo.province_id, null)
        if (provinceJsonObj != null) {
          orderInfo.province_name = provinceJsonObj.getString("NAME")
          orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
          orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
        }
        orderInfo
      }
      rddWithProvince
    }

    // 还要合并用户的维度的数据
    // 此处作为作业开发



   // orderInfoWithProvinceDstream.print(1000)


    //写入操作
    // 1  更新  用户状态  phoenix
    // 2  存储olap  用户分析    可选  es
    // 3  推kafka 进入下一层处理   可选  主题： DWD_ORDER_INFO
    // 4  提交偏移量
     orderInfoWithProvinceDstream.foreachRDD { rdd =>
      rdd.cache()
       // 1  更新  用户状态  phoenix
        val userStateRDD: RDD[UserState] = rdd.map(orderInfo => UserState(orderInfo.user_id.toString, "1"))
//      //spark phoenix的整合工具
       userStateRDD.saveToPhoenix("USER_STATE0213",
        Seq("USER_ID", "IF_CONSUMED"),
        new Configuration,
        Some("hdp1,hdp2,hdp3:2181"))
////
////
       // 2  存储olap  用户分析    可选  es  建立索引
     rdd.foreachPartition { orderInfoItr =>
//
        val orderInfoList: List[(OrderInfo, String)] = orderInfoItr.toList.map(orderInfo => (orderInfo, orderInfo.id.toString))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
     //   MyEsUtil.bulkSave(orderInfoList, "gmall0213_order_info_" + dateStr)

       // 3  推kafka 进入下一层处理   可选  主题： DWD_ORDER_INFO
        for ((orderInfo, id) <- orderInfoList) {  //fastjson 要把scala对象包括caseclass转json字符串 需要加入,new SerializeConfig(true)
                                                  //json4s scala专用工具
          MyKafkaSink.send("DWD_ORDER_INFO", id, JSON.toJSONString(orderInfo,new SerializeConfig(true)))
        }

     }
       // 4  提交偏移量 //driver中执行
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
     }
    ssc.start()
    ssc.awaitTermination()

  }
}
