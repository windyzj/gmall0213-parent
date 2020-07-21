package com.atguigu.gmall0213.realtime.util

import java.util

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.mutable.ListBuffer

object MyEsUtil {


  private    var  factory:  JestClientFactory=null;

  def getJestClient(): JestClient ={
    if(factory!=null){
      factory.getObject
    }else{
      build()
      factory.getObject
    }
  }

  def build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig((new HttpClientConfig.Builder("http://hdp1:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build()
      ))
  }

  def saveToEs(): Unit ={
    val jestClient: JestClient = getJestClient()
    // 写操作
    val index = new Index.Builder(Movie("0103","复仇者联盟")).index("movie_test0213_20200721").`type`("_doc").id("0103").build()
    jestClient.execute(index)
    jestClient.close()
  }

  def queryFromEs(): Unit ={
    val jestClient: JestClient = getJestClient()
    val query="{\n \"query\": {\n    \"match\": {\n      \"name\": \"red\"\n    }\n  },\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"asc\"\n      }\n    }\n  ],\n    \"from\": 0,\n  \"size\": 20 \n\n}";
    val searchSourceBuilder = new SearchSourceBuilder
    searchSourceBuilder.query(new MatchQueryBuilder("name","red"))
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(20)
    val query2: String = searchSourceBuilder.toString
    println(query2)
    val search: Search = new Search.Builder(query2).addIndex("movie_index0213").addType("movie").build()
    val result: SearchResult = jestClient.execute(search )
    val resultList: util.List[SearchResult#Hit[util.Map[String, Object], Void]] = result.getHits(classOf[ util.Map[String,Object]])
   // val finalList:ListBuffer[Map[String,Object]]=new ListBuffer[Map[String,Object]]
    import collection.JavaConversions._
    for (hit <- resultList ) {
      val source: util.Map[String, Object] = hit.source
      println(source)
    }

    jestClient.close()
  }



  def main(args: Array[String]): Unit = {
    //saveToEs()
    // query 操作
    queryFromEs()


  }

  case class Movie(id:String ,movie_name:String)

}
