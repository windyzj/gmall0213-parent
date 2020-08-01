package com.atguigu.gmall0213.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import java.math.BigDecimal

import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.spark.storage.StorageLevel

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

    // 因为在处理分摊金额之前 已经根据order_id 进行了join （shuffle） 所以 目前的分区中不可能存在相同的order_id 在不同的分区中

    val orderWideWithSplitDstream: DStream[OrderWide] = orderWideDstream.mapPartitions { orderWideItr =>
      //建连接
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: List[OrderWide] = orderWideItr.toList
      println("分区orderIds:" + orderWideList.map(_.order_id).mkString(","))
      //迭代
      for (orderWide <- orderWideList) {
        // 判断计算方式
        //如何判断最后一笔 ? 如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价）
        // 个数 单价  原始金额 可以从orderWide取到
        // 要从redis中取得累计值   Σ其他的明细（个数*单价）
        // redis    type?  string    key?  order_origin_sum:[order_id]  value? Σ其他的明细（个数*单价）
        val originSumKey = "order_origin_sum:" + orderWide.order_id
        var orderOriginSum: Double = 0D
        var orderSplitSum: Double = 0D
        val orderOriginSumStr = jedis.get(originSumKey)
        //从redis中取出来的任何值都要进行判空
        if (orderOriginSumStr != null && orderOriginSumStr.size > 0) {
          orderOriginSum = orderOriginSumStr.toDouble
        }
        //如果等式成立 说明该笔明细是最后一笔  （如果当前的一笔 个数*单价== 原始总金额-  Σ其他的明细（个数*单价））
        val detailAmount = orderWide.sku_price * orderWide.sku_num
        if (detailAmount == orderWide.original_total_amount - orderOriginSum) {
          // 分摊计算公式 :减法公式  分摊金额= 实际付款金额-  Σ其他的明细的分摊金额  (减法，适用最后一笔明细）
          // 实际付款金额 在orderWide中，
          // 要从redis中取得  Σ其他的明细的分摊金额
          // redis    type?  string    key?  order_split_sum:[order_id]  value? Σ其他的明细的分摊金额

          val orderSplitSumStr: String = jedis.get("order_split_sum:" + orderWide.order_id)
          if (orderSplitSumStr != null && orderSplitSumStr.size > 0) {
            orderSplitSum = orderSplitSumStr.toDouble
          }
          orderWide.final_detail_amount =   Math.round( (orderWide.final_total_amount - orderSplitSum)*100D)/100D

        } else {
          //如果不成立
          // 分摊计算公式： 乘除法公式： 分摊金额= 实际付款金额 *(个数*单价) / 原始总金额   （乘除法，适用非最后一笔明细)
          //  所有计算要素都在 orderWide 中，直接计算即可
          orderWide.final_detail_amount = Math.round( (orderWide.final_total_amount * detailAmount / orderWide.original_total_amount)*100D)/100D
        }
        // 分摊金额计算完成以后

        //  要本次计算的分摊金额 累计到redis    Σ其他的明细的分摊金额
        val newOrderSplitSum = (orderWide.final_detail_amount + orderSplitSum).toString
        jedis.setex("order_split_sum:" + orderWide.order_id, 600, newOrderSplitSum)

        //   应付金额（单价*个数) 要累计到   Σ其他的明细（个数*单价）
        val newOrderOriginSum = (detailAmount + orderOriginSum).toString
        jedis.setex(originSumKey, 600, newOrderOriginSum)

      }
      // 关闭redis
      jedis.close()

      //返回一个计算完成的 list的迭代器

      // 当多个分区的数据 可能会同时访问某一个数值（累计值）的情况 ，可能会出现并发问题
      // 1  本身数据库是否有并发控制 redis 单线程  mysql行锁
      //2   让涉及并发的数据存在于同一个分区中   2.1  进入kafka的时候指定分区键  2.2 在spark 利用dstream[k,v].partitionby(new HashPartitioner(partNum)) shuffle

      orderWideList.toIterator
    }

    orderWideWithSplitDstream.persist(StorageLevel.MEMORY_ONLY)
     orderWideWithSplitDstream.print(1000)




    //jdbc sql

    val sparkSession: SparkSession = SparkSession.builder().appName("dws_order_wide_app")getOrCreate()

    import  sparkSession.implicits._
    orderWideWithSplitDstream.foreachRDD{rdd=>
      rdd.cache()
      val df: DataFrame = rdd.toDF()
      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "4") // 设置并发
        .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
        .jdbc("jdbc:clickhouse://hdp1:8123/test0213","order_wide_0213",new Properties())

      rdd.foreach{orderWide=>
          MyKafkaSink.send("DWS_ORDER_WIDE",  JSON.toJSONString(orderWide,new SerializeConfig(true)))

      }


      OffsetManager.saveOffset(orderInfoTopic,orderInfoGroupId,orderInfoOffsetRanges)
      OffsetManager.saveOffset(orderDetailTopic,orderDetailGroupId,orderDetailOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()










  }








}
