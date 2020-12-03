package com.atguigu.udf

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-21 23:07
 */
/*
表函数：
自定义TableFunction：需求分割字符串，统计每个子串的字符个数
 1. TableFunction<T> 泛型为输出数据类型
 2.  protected final void collect(T row) {
        this.collector.collect(row);
    }
 3. 导入scala隐式转换的包：
    1）import org.apache.flink.streaming.api.scala._
    2）import org.apache.flink.table.api.scala._
 4.lateral table(split(id)) as tableName(word,length) 相当于原表每一行数据与炸裂后的表进行笛卡尔积
 5. 自定义表函数的过程：
  1）定义一个类继承TableFunction<T>，T为输入数据类型；
  2）定义一个方法def eval(str:String):Unit={} 方法名必须为eval，返回值类型必须为Unit；
  3）Table API使用自定义表函数的步骤：
    a.创建自定义TableFunction对象
    b.使用Table进行joinLateral（侧写视图，炸裂字段），再select字段
  4）SQL语法
    a.创建自定义TableFunction对象
    b.注册表和注册函数
    c.使用表环境执行SQL  from 表名, lateral table(split(炸裂字段)) as 临时表名(炸裂后的字段名1,炸裂后的字段名2)
 */
object Test02_TableFunction {
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

    // 5.table api
    // a.创建自定义TableFunction对象
    val split = new Split("_")
    // b.使用Table进行joinLateral（侧写视图，炸裂字段），在select字段
    val resultTable = sensorTable
      .joinLateral(split('id) as('word,'length))  // 侧写视图，炸裂字段
      .select('id,'ts,'word,'length) //

    // 6.sql
    // a.注册表
    tableEnv.createTemporaryView("sensor",sensorTable)
    // b.注册函数
    tableEnv.registerFunction("split",split)
    // c.执行sql
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id,ts,word,length
        |from sensor, lateral table(split(id)) as tableName(word,length)
        |""".stripMargin)

    // 7.打印,都是插入操作
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    // 8.执行任务
    env.execute()
  }
}

//自定义TableFunction：需求分割字符串，统计每个子串的字符个数
class Split(seperator :String) extends TableFunction[(String,Int)]{

  def eval(str:String):Unit={
    str.split(seperator).foreach(
      word => collect((word,word.length))
    )
  }
}