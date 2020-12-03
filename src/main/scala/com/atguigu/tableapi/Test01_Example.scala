package com.atguigu.tableapi


import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-19 14:20
 */
/*
Table API:
1. 导包以便隐式转换import org.apache.flink.table.api.scala._
2. 使用sql必须先创建表(基于流DataStream)，再注册表（基于Table类型注册），形成catalog，最后使用table环境执行sql，
    输出时转换成流（resultTable.toAppendStream[(String,Double)]）输出。
3. 使用table API：先创建表执行环境，再基于流创建一张表（Table类型），之后进行table API的转换，输出时转换成流输出。
 */
object Test01_Example {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
    val inputDataStream = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 4.首先创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 5.基于流创建一张表
   val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 6.调用table api进行转换
    val resultTable = dataTable
      .select("id,temperature")
      // 注意：是==，以及单引号。
      .filter("id == 'sensor_1'")

    // 7.直接用sql实现
    // 7.1 注册表
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sql:String ="select id, temperature from dataTable where id = 'sensor_1'"
    // 7.2 执行sql语句
    val resultSqlTable = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("resultSqlTable")

    // 4.执行任务
    env.execute("Test01_Example")

  }
}
