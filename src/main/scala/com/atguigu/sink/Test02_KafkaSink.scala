package com.atguigu.sink

import java.util.Properties

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @author chenhuiup
 * @create 2020-11-15 11:21
 */
/*
kafka数据管道的构建：
从kafka中读取数据，经过Flink的转换再写回到kafka中。Flink中可以做数据清洗。
 */
object Test02_KafkaSink {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
//    val inputDataStream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 6. 从kafka中读取数据
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"uzi")

    val kafkaDataStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]
      ("sensor",new SimpleStringSchema(),props))

    // 3.转换数据结构
    val dataStream: DataStream[String] = kafkaDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      })

    // 5.向kafka中写入数据
    dataStream.addSink(
      new FlinkKafkaProducer011[String](
        "hadoop102:9020",
        "sensor",
        // 序列化器
        new SimpleStringSchema()
      )
    )

    // 4.任务执行
    env.execute("Test02_KafkaSink")
  }

}
