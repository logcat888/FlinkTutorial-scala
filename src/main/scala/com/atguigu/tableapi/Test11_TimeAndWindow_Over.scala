package com.atguigu.tableapi

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Over, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-21 20:52
 */
/*
使用Over window实现：统计每个sensor每条数据，与之前两行数据的平均温度
1. 与groupBy一样over window的结果也只是追加操作，使用toAppendStream[Row]或者toRetractStream[Row]都行
 */
object Test11_TimeAndWindow_Over {
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

    // 5. Over window:统计每个sensor每条数据，与之前两行数据的平均温度
    // 5.1 table api
    val overResultTable = sensorTable
        .window( Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow) // 开窗
        .select('id ,'ts, 'id.count over 'ow ,'temperature.avg over 'ow) // 窗口函数 + 窗口

    // 5.2 sql
    // a.注册表
    tableEnv.createTemporaryView("sensor",sensorTable)
    // b.执行sql
    val overResultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | ts,
        | count(id) over ow,
        | avg(temperature) over ow
        |from sensor
        |window ow as(
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)

    // 6.打印
    overResultTable.toAppendStream[Row].print("result")
    overResultSqlTable.toRetractStream[Row].print("sql")

    // 7.执行
    env.execute("Test07_TimeAndWindow")
  }
}
