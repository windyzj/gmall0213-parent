package com.atguigu.gmall0213.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

object OffsetManager {


  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {
    ///  redis  type? hash   key  ? 主题1：消费者组1  field ?  分区 value ?偏移量
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey = topic + ":" + consumerGroupId

    var offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    // scala 2.11.8   scala 2.12.11

    if (offsetMap != null && offsetMap.size > 0) {
      val offsetList: List[(String, String)] = offsetMap.toList

      val offsetListForKafka: List[(TopicPartition, Long)] = offsetList.map { case (partition, offset) =>
        val topicPartition = new TopicPartition(topic, partition.toInt)
        (topicPartition, offset.toLong)
      }
      val offsetMapForKafka: Map[TopicPartition, Long] = offsetListForKafka.toMap

      offsetMapForKafka
    } else {
      null
    }

  }

  def saveOffset(): Unit = {

  }

}
