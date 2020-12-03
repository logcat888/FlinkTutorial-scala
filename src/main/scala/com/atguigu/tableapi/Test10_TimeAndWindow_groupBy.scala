package com.atguigu.tableapi

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-20 7:35
 */
/*
分组窗口：
  1）使用分组窗口进行聚合操作，最后转换为流输出时，都是追加操作，使用toAppendStream[Row]和toRetractStream[Row]都行
over window：

 */
object Test10_TimeAndWindow_groupBy {
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

    // 5. Group Window
    // 5.1 table api
   val resultTable =  sensorTable
//      .window(Tumble.over(10.seconds).on('ts).as('tw) ) 在scala中 可以省略. 用空格代替
            .window(Tumble over 10.seconds on 'ts as 'tw) // 每10秒钟统计一次，滚动时间窗口
        .groupBy('id,'tw)
        .select('id,'id.count,'temperature.avg,'tw.end) //'tw.end代表窗口的结束时间

    // 5.2 sql
    // a.注册表
    tableEnv.createTemporaryView("sensor",sensorTable)
    // b.执行sql
    val resulSqltTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | count(id),
        | avg(temperature),
        | tumble_end(ts,interval '10' second)
        | from sensor
        | group by
        |   id,
        |   tumble(ts,interval '10' second)
        |""".stripMargin)

    // 转换成流打印输出
    resultTable.toAppendStream[Row].print("resultTable")
    resulSqltTable.toRetractStream[Row].print("resulSqltTable")
    /*
    注：都是追加操作，使用toAppendStream[Row]和toRetractStream[Row]都行
resultTable> sensor_1,1,35.8,2019-01-17T09:43:20
resultTable> sensor_6,1,15.4,2019-01-17T09:43:30
resultTable> sensor_1,2,34.1,2019-01-17T09:43:30
resultTable> sensor_10,1,38.1,2019-01-17T09:43:30
resultTable> sensor_7,1,6.7,2019-01-17T09:43:30
resultTable> sensor_1,2,30.299999999999997,2019-01-17T09:43:40
resulSqltTable> (true,sensor_1,1,35.8,2019-01-17T09:43:20)
resulSqltTable> (true,sensor_6,1,15.4,2019-01-17T09:43:30)
resulSqltTable> (true,sensor_1,2,34.1,2019-01-17T09:43:30)
resulSqltTable> (true,sensor_10,1,38.1,2019-01-17T09:43:30)
resulSqltTable> (true,sensor_7,1,6.7,2019-01-17T09:43:30)
resulSqltTable> (true,sensor_1,2,30.299999999999997,2019-01-17T09:43:40)
     */
    
    // 6.执行
    env.execute("Test07_TimeAndWindow")
  }

}
