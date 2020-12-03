package com.atguigu.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @author chenhuiup
 * @create 2020-11-19 21:11
 */
/*
kafka数据管道，从kafka读取数据转换为Table，转换后，再写入kafka中
1. kafka只实现了AppendStreamTableSink，由于kafka是消息队列，先进先出，记录这每条消息的offset，不支持删除操作，所以不能将
  聚合后的结果写入到kafka中。
2. kafka不支持Retract模式和upsert模式
public abstract class KafkaTableSinkBase implements AppendStreamTableSink<Row> {
 */
object Test05_KafkaPipeline {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 2.从kafka读取数据
    tableEnv.connect( new Kafka()
      .topic("flink0621")
      .version("0.11") //kafka版本
      .property("zookeeper.connect","hadoop102:2181")
      .property("bootstrap.server","hadoop102:9092")) // 定义表数据来源，外部连接
      .withFormat(new Csv())  //定义从外部系统读取之后的格式化方法
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )  //定义表结构
      .createTemporaryTable("kafkaTable") //创建临时表

    // 3.转换操作
    val sensorTable: Table = tableEnv.from("kafkaTable")

    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable: Table = sensorTable
      //  tab.groupBy('key).select('key, 'value.avg + " The average" as 'average)
      .groupBy('id) //基于id分组
      .select('id, 'id.count as 'count) //分组后组内计数聚合，并起别名

    // 4 输出到kafka
    tableEnv.connect( new Kafka()
      .topic("sensor")
      .version("0.11") //kafka版本
      .property("zookeeper.connect","hadoop102:2181")
      .property("bootstrap.server","hadoop102:9092")) // 定义表数据来源，外部连接
      .withFormat(new Csv())  //定义从外部系统读取之后的格式化方法
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
      )  //定义表结构
      .createTemporaryTable("kafkaOutputTable") //创建临时表

    resultTable.insertInto("kafkaOutputTable")

    // 3.执行任务
    env.execute("Test05_KafkaPipeline")
  }
}
