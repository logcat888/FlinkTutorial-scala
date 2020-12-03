package com.atguigu.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
 * @author chenhuiup
 * @create 2020-11-19 17:21
 */
/*
从kafka中读取数据
 */
object Test03_Table_Kafka {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)


    // 2. 连接外部系统，读取数据，注册表
    // 2.1 从kafka读取数据
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

    // 2.2基于已经创建的表，转换为Table类型
    val inputTable: Table = tableEnv.from("inputTable")

    // 2.3将Table类型转换为流，进行输出
    inputTable.toAppendStream[(String,Long,Double)].print()


    // 4.执行任务
    env.execute("Test03_Table_Kafka")
  }

}
