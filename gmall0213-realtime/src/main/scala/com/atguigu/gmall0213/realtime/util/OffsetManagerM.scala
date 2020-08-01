package com.atguigu.gmall0213.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

object OffsetManagerM {


  def getOffset(topic: String, consumerGroupId: String): Map[TopicPartition, Long] = {
    val sql=" select group_id,topic,topic_offset,partition_id from offset_0213 where topic='"+topic+"' and group_id='"+consumerGroupId+"'"

    val jsonObjList: List[JSONObject] = MySqlUtil.queryList(sql)

    val topicPartitionList: List[(TopicPartition, Long)] = jsonObjList.map { jsonObj =>
      val topicPartition: TopicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
      val offset: Long = jsonObj.getLongValue("topic_offset")
      (topicPartition, offset)
    }
    val topicPartitionMap: Map[TopicPartition, Long] = topicPartitionList.toMap

    topicPartitionMap
  }


  }
