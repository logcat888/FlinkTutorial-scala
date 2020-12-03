package com.atguigu.udf

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-21 22:12
 */
/*
自定义标量函数（UDF函数）实现HashCode:

1. table api:
  1）创建一个类继承ScalarFunction，并定义eval方法实现逻辑
  2）创建自定义的标量函数的对象
  3）在table api 中直接使用对象
2. sql
  1)注册表
  2）注册函数
  3）执行sql
 */
object Test01_ScalarFunction {
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

    // 调用自定义hash函数，对id进行hash运算
    // 1. table api
    // 首先new 一个UDF的实例
    val hashCode = new HashCode(23)
    val resultTable = sensorTable
      .select('id, 'ts, hashCode('id))

    // 2.sql
    // a.注册表
    tableEnv.createTemporaryView("sensor",sensorTable)
    // b.需要在环境中注册UDF
    tableEnv.registerFunction("hashCode",hashCode)
    // c.执行SQL
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,
        | ts,
        | hashCode(id)
        |from sensor
        |""".stripMargin)

    // 打印
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    // 6.执行任务
    env.execute("Test01_ScalarFunction")
  }

}

//自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }

}