package com.atguigu.tableapi

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
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
 */
object Test07_TimeAndWindow_ProcessingTime {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置时间语义,默认是处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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
      })

    // 4. 从流中创建表时指定时间特性,可以调换字段顺序，并追加时间戳字段成为伪列
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp, 'pt.proctime)

    // 5.打印表结构
    sensorTable.printSchema()
    // 将动态表中的一行数据转换为Row类型的流进行打印输出
    sensorTable.toAppendStream[Row].print()
/*

注：表结构，TIMESTAMP(3)代表时间精度为毫秒，TIMESTAMP(6)代表时间精度为微秒
root
 |-- id: STRING
 |-- temperature: DOUBLE
 |-- timestamp: BIGINT
 |-- pt: TIMESTAMP(3) *PROCTIME*

注：追加的时间戳默认是UTC：0时区的时间，比中国晚8小时
sensor_1,35.8,1547718199,2020-11-19T23:50:26.708
sensor_6,15.4,1547718201,2020-11-19T23:50:26.708
sensor_7,6.7,1547718202,2020-11-19T23:50:26.708
sensor_10,38.1,1547718205,2020-11-19T23:50:26.708
sensor_1,32.0,1547718206,2020-11-19T23:50:26.708
sensor_1,36.2,1547718208,2020-11-19T23:50:26.708
sensor_1,29.7,1547718210,2020-11-19T23:50:26.708
sensor_1,30.9,1547718213,2020-11-19T23:50:26.709
 */
    // 6.执行
    env.execute("Test07_TimeAndWindow")
  }

}
