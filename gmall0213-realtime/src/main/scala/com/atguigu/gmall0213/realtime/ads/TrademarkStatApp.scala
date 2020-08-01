package com.atguigu.gmall0213.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0213.realtime.bean.{OrderDetail, OrderWide}
import com.atguigu.gmall0213.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

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

    tradermarkSumDstream.foreachRDD { rdd =>
      // 把ex的数据提取到driver中
      val tmSumArr: Array[(String, Double)] = rdd.collect()
      //driver 执行 还是 executor?
      if (tmSumArr.size > 0) {
        DBs.setup()
        DB.localTx { implicit session =>
          // 写入计算结果数据
          val formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val dateTime: String = formator.format(new Date())
          val dt = dateTime.split(" ")(0)
          var batchParams = Seq[Seq[Any]]()
          val batchParamsList: ListBuffer[Seq[Any]] = ListBuffer[Seq[Any]]()
          for ((tm, amount) <- tmSumArr) {
            val amountRound: Double = Math.round(amount * 100D) / 100D
            val tmArr: Array[String] = tm.split("_")
            val tmId = tmArr(0)
            val tmName = tmArr(1)
            batchParamsList.append(Seq(dt, dateTime, tmId, tmName, amountRound))
          }
//           val params: Seq[Seq[Any]] = Seq(Seq("2020-08-01","2020-08-01 10:10:10","101","品牌1",2000.00),Seq("2020-08-01","2020-08-01 10:10:10","102","品牌2",3000.00))
          //数据集合作为多个可变参数 的方法 的参数的时候 要加:_*
          SQL("insert into trademark_amount_stat(stat_date,stat_time,tm_id,tm_name,amount) values(?,?,?,?,?)").batch(batchParamsList.toSeq:_*).apply()
          //throw new RuntimeException("测试异常")
          // 写入偏移量
          for (offsetRange <- offsetRanges) {
            val partitionId: Int = offsetRange.partition
            val untilOffset: Long = offsetRange.untilOffset
            SQL("replace into offset_0213  values(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()
          }
        }
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }


}
