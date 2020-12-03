package com.atguigu.sink

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @author chenhuiup
 * @create 2020-11-15 14:19
 */
/*
使用ElasticSearchSink将数据写入Es中
 */
object Test04_EsSink {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
    val inputDataStream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 定义HttpHosts, 使用java的list装多个es节点
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200))
    httpHosts.add(new HttpHost("hadoop103",9200))

    //自定义写入es的EsSinkFunction，使用匿名子类的方式
    val myEsSinkFunc= new ElasticsearchSinkFunction[SensorReading]{
        // 每来一条数据都会调用一次process方法
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          //包装一个Map作为data source, 因为Es数据的输入和输出都是Json字符串
          val dataSource = new util.HashMap[String,String]()
          dataSource.put("id",t.id)
          dataSource.put("temperature",t.temperature.toString)
          dataSource.put("ts",t.timestamp.toString)

          // 创建index request，用于发送http请求
          val indexRequest: IndexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingData")
            .source(dataSource)

          // 用indexer发送请求
          requestIndexer.add(indexRequest)
        }
      }

    dataStream.addSink(new ElasticsearchSink
    .Builder[SensorReading](httpHosts,myEsSinkFunc)
    .build())

    // 4.任务执行
    env.execute("Test04_EsSink")
  }

}
