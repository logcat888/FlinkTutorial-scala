package com.atguigu.tableapi

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-20 7:35
 */
/*
时间语义及窗口：
1.定义处理时间属性有三种方法：
  1）在DataStream转化时直接指定；
  2）在定义Table Schema时指定；
  3）在创建表的DDL中指定。
2.定义事件时间属性有三种方法：
  1）在DataStream转化时直接指定；

  2）在定义Table Schema时指定；
  3）在创建表的DDL中指定。

 */
object Test09_TimeAndWindow_EventTime02 {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() //在1.10默认使用old planner，在1.11默认使用BlinkPlanner
      .inStreamingMode() //使用流处理环境
      .build()

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 4.	定义Table Schema时指定
    tableEnv.connect(
      new FileSystem().path("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .rowtime( // 插在中间定义
          new Rowtime()
            .timestampsFromField("timestamp")    // 从字段中提取时间戳
            .watermarksPeriodicBounded(1000)    // watermark延迟1秒
        )
        .field("temperature", DataTypes.DOUBLE())
      ) // 定义表结构
      .createTemporaryTable("inputTable") // 创建临时表

    val table = tableEnv.from("inputTable")

    // 5.打印表结构
    table.printSchema()
    // 将动态表中的一行数据转换为Row类型的流进行打印输出
    table.toAppendStream[Row].print()
/*

 */
    // 6.执行
    env.execute("Test07_TimeAndWindow")
  }

}
