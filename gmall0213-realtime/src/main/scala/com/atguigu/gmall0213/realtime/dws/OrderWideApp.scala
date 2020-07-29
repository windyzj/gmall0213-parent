package com.atguigu.gmall0213.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderWideApp {



  def main(args: Array[String]): Unit = {
    //双流  订单主表  订单从表    偏移量 双份
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dws_order_wide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoGroupId = "dws_order_info_group"
    val orderInfoTopic = "DWD_ORDER_INFO"
    val orderDetailGroupId = "dws_order_detail_group"
    val orderDetailTopic = "DWD_ORDER_DETAIL"

    //1   从redis中读取偏移量   （启动执行一次）
    val orderInfoOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderInfoTopic, orderInfoGroupId)
    val orderDetailOffsetMapForKafka: Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailTopic, orderDetailGroupId)

    //2   把偏移量传递给kafka ，加载数据流（启动执行一次）
    var orderInfoRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsetMapForKafka != null && orderInfoOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoOffsetMapForKafka, orderInfoGroupId)
    } else {
      orderInfoRecordInputDstream = MyKafkaUtil.getKafkaStream(orderInfoTopic, ssc, orderInfoGroupId)
    }


    var orderDetailRecordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsetMapForKafka != null && orderDetailOffsetMapForKafka.size > 0) { //根据是否能取到偏移量来决定如何加载kafka 流
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetMapForKafka, orderDetailGroupId)
    } else {
      orderDetailRecordInputDstream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
    }


    //3   从流中获得本批次的 偏移量结束点（每批次执行一次）
    var orderInfoOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = null //周期性储存了当前批次偏移量的变化状态，重要的是偏移量结束点
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputDstream.transform { rdd => //周期性在driver中执行
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 1 提取数据 2 分topic
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      //订单处理  脱敏  换成特殊字符  直接去掉   转换成更方便操作的专用样例类
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    //
//    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo=>(orderInfo.id,orderInfo))
//    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))
//
//    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)



    // 方案一：  1开窗口    2 join 3  去重
    //window
    val orderInfoWindowDstream: DStream[OrderInfo] = orderInfoDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长
    val orderDetailWindowDstream: DStream[OrderDetail] = orderDetailDstream.window(Seconds(15), Seconds(5)) //窗口大小  滑动步长

    // join
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWindowDstream.map(orderInfo=>(orderInfo.id,orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWindowDstream.map(orderDetail=>(orderDetail.order_id,orderDetail))

    val joinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream,4)

    // 去重
    //  数据统一保存到
    // redis ?  type? set   api? sadd   key ? order_join:[orderId]        value ? orderDetailId (skuId也可）  expire : 60*10
    // sadd 返回如果0  过滤掉
    val orderWideDstream: DStream[OrderWide] = joinedDstream.mapPartitions { tupleItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      for ((orderId, (orderInfo, orderDetail)) <- tupleItr) {
        val key = "order_join:" + orderId
        val ifNotExisted: lang.Long = jedis.sadd(key, orderDetail.id.toString)
        jedis.expire(key, 600)
        //合并宽表
        if (ifNotExisted == 1L) {
          orderWideList.append(new OrderWide(orderInfo, orderDetail))
        }
      }
      jedis.close()
      orderWideList.toIterator
    }

    //orderWideDstream.print(1000)

    // //按比例求分摊: 分摊金额/实际付款金额 = 个数*单价  /原始总金额
    // 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
    //         最后一笔 = 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
    // 如何判断最后一笔 ？如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
    //   利用 redis(mysql)  保存计算完成的累计值  Σ其他的明细的分摊金额 Σ其他的明细（个数*单价）

    orderWideDstream.mapPartitions{orderWideItr=>
      //建连接
      //迭代
      for (orderWide <- orderWideItr ) {
        // 判断计算方式
        //如何判断最后一笔 ? 如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
        // 个数 单价  原始金额 可以从orderWide取到
        // 要从redis中取得累计值   Σ其他的明细（个数*单价）
        // redis    type?  string    key?  order_origin_sum:[order_id]  value? Σ其他的明细（个数*单价）
        //如果等式成立 说明该笔明细是最后一笔
                    // 分摊计算公式 :减法公式  分摊金额= 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
                                       // 实际付款金额 在orderWide中，
                                        // 要从redis中取得  Σ其他的明细的分摊金额
                                         // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额
        //如果不成立
                   // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
                                     //  所有计算要素都在 orderWide 中，直接计算即可
          // 分摊金额计算完成以后
          // 要赋给orderWide中分摊金额
           //  要本次计算的分摊金额 累计到redis    Σ其他的明细的分摊金额
           //   应付金额（单价*个数) 要累计到   Σ其他的明细（个数*单价）

      }
     // 关闭redis
      //返回一个计算完成的 list的迭代器

      null
    }






    //jdbc sql

    val sparkSession: SparkSession = SparkSession.builder().appName("dws_order_wide_app")getOrCreate()

    import  sparkSession.implicits._
    orderWideDstream.foreachRDD{rdd=>
      val df: DataFrame = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "4") // 设置并发
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hdp1:8123/test0213","order_wide_0213",new Properties())



      OffsetManager.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
      OffsetManager.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()










  }








}
