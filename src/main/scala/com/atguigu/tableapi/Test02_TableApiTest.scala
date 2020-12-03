package com.atguigu.tableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

/**
 * @author chenhuiup
 * @create 2020-11-19 16:04
 */
/*
测试table api
1. 老版本的批流是基于不同的执行环境，批处理是基于批处理的执行环境；blink是真正的批流统一的api，都基于流式执行环境。
2. 设置不同planner及相应的批流处理
    2.1 old planner：
        1）流处理
        2）批处理
    2.2 blink planner
        1）流处理
        2）批处理
3. 创建表（从文件读取数据）
  1）连接外部系统，读取数据，注册表
    > 连接外部系统
    > 设置格式
    > 定义表结构
    > 创建临时表
  2） 从kafka读取数据
4. Table API 与 SQL
 1）   .select('id, 'temperature) // 支持传入表达式senb，'id 代表字段
      .filter('id === "sensor_1") // === 是ImplicitExpressionOperations中定义的等于含义
 */
object Test02_TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    /*
    // 1.1 基于老版本planner的流处理（默认也是old planner）

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 使用老版本planner
      .inStreamingMode() // 流处理模式
      .build()

    StreamTableEnvironment.create(env,settings)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner的流处理
    val blinkStreamSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val blinkStreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,blinkStreamSetting)

    // 1.4 基于blink planner的批处理
    val blinkBatchSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val blinkBatchTableEnv: StreamTableEnvironment = TableEnvironment.create(blinkBatchSetting)
*/

    // 2. 连接外部系统，读取数据，注册表
    // 2.1 读取文件
    val filePath = "D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect( new FileSystem().path(filePath)) // 定义表数据来源，外部连接
//      .withFormat(new OldCsv())  //定义从外部系统读取之后的格式化方法
      .withFormat(new Csv())  //定义从外部系统读取之后的格式化方法
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )  //定义表结构
      .createTemporaryTable("inputTable") //创建临时表

    // 2.2基于已经创建的表，转换为Table类型
//    val inputTable: Table = tableEnv.from("inputTable")
//
//    // 2.3将Table类型转换为流，进行输出
//    inputTable.toAppendStream[(String,Long,Double)].print()

     // 2.2 从kafka读取数据
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

    // 3. 查询转换
    // 3.1 使用table api
    val sensorTable = tableEnv.from("inputTable")
    val resultTable: Table = sensorTable
      .select('id, 'temperature) // 支持传入表达式senb，'id 代表字段
      .filter('id === "sensor_1") // === 是ImplicitExpressionOperations中定义的等于含义

    // 3.2使用SQL
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin)

    // 打印输出
    resultTable.toAppendStream[(String,Double)].print("resultTable")
    resultSqlTable.toAppendStream[(String,Double)].print("resultSqlTable")



    // 4.执行任务
    env.execute("Test02_TableApiTest")


  }
}
