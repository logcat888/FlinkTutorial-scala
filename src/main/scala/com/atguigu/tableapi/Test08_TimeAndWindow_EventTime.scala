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
object Test08_TimeAndWindow_EventTime {
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

    // 2.读取数据
    val inputDataStream = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }) //提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    })

    // 4. 从流中创建表时指定时间特性,可以调换字段顺序，并追加时间戳字段成为伪列
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts )

    // 5.打印表结构
    sensorTable.printSchema()
    // 将动态表中的一行数据转换为Row类型的流进行打印输出
    sensorTable.toAppendStream[Row].print()
/*
注：可以看到覆盖后的timestamp是 TIMESTAMP(3)类型
root
 |-- id: STRING
 |-- temperature: DOUBLE
 |-- timestamp: TIMESTAMP(3) *ROWTIME*

注：从数据中提取时间戳，注意时间是2019-01-17，现在是2020-11-20
sensor_1,35.8,2019-01-17T09:43:19
sensor_6,15.4,2019-01-17T09:43:21
sensor_7,6.7,2019-01-17T09:43:22
sensor_10,38.1,2019-01-17T09:43:25
sensor_1,32.0,2019-01-17T09:43:26
sensor_1,36.2,2019-01-17T09:43:28
sensor_1,29.7,2019-01-17T09:43:30
sensor_1,30.9,2019-01-17T09:43:33
 */
    // 6.执行
    env.execute("Test07_TimeAndWindow")
  }

}
