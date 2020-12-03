package com.atguigu.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @author chenhuiup
 * @create 2020-11-19 19:50
 */
/*
使用Table API 和 Flink SQL输出到文件中
1. 为了隐式转换方便，导包的时候
  import org.apache.flink.streaming.api.scala._
  import org.apache.flink.table.api.scala._
2. toAppendStream是追加流，对于聚合之后的表，不能使用追加流转换输出，新来的数据应该与原表的数据再次进行聚合，而不是追加到表的后面。
    toRetractStream:聚合之后的表想输出必须使用toRetractStream转换进行更新操作
3. CsvTableSink只实现了AppendStreamTableSink，只能进行追加操作，如果是聚合后的表不能使用其写入外部文件系统，因为写入到文件
  系统只能追加，不能更改
  public class CsvTableSink implements BatchTableSink<Row>, AppendStreamTableSink<Row>
4. 对于文件系统，聚合后的表不能写入其他，聚合后的表只能写入到能够修改的数据库，其sink必须实现RetractStreamTableSink 或者
  UpdateStreamTableSink
 */
object Test04_FileOutput {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

//    2. 连接外部系统，读取数据，注册表
    // 2.1 读取文件
    val filePath = "D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect( new FileSystem().path(filePath)) // 定义表数据来源，外部连接
      .withFormat(new Csv())  //定义从外部系统读取之后的格式化方法
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temp",DataTypes.DOUBLE())
      )  //定义表结构
      .createTemporaryTable("inputTable") //创建临时表

    // 3.转换操作
    val sensorTable: Table = tableEnv.from("inputTable")

    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable: Table = sensorTable
      //  tab.groupBy('key).select('key, 'value.avg + " The average" as 'average)
      .groupBy('id) //基于id分组
      .select('id, 'id.count as 'count) //分组后组内计数聚合，并起别名


    // 普通输出toAppendStream
//    val resultTable01: DataStream[(String, Double)] = resultTable.toAppendStream[(String,Double)]
//    resultTable01.print("resultTable")
//    // 分组聚合后输出，toRetractStream
//    val aggTable01: DataStream[(Boolean, (String, Long))] = aggTable.toRetractStream[(String,Long)]
//    aggTable01.print("aggTable")


    /*
    聚合后通过true和false表示对于新来的数据的更新，对于之前的数据无法修改，所以使用false表示作废，使用true表示聚合更新后的结果
    resultTable> (sensor_1,35.8)
    resultTable> (sensor_1,32.0)
    resultTable> (sensor_1,36.2)
    resultTable> (sensor_1,29.7)
    resultTable> (sensor_1,30.9)
    aggTable> (true,(sensor_1,1))
    aggTable> (true,(sensor_6,1))
    aggTable> (true,(sensor_7,1))
    aggTable> (true,(sensor_10,1))
    aggTable> (false,(sensor_1,1))
    aggTable> (true,(sensor_1,2))
    aggTable> (false,(sensor_1,2))
     */

    // 4.输出到文件中
    // 4.1 先注册表
    val outputPath = "D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\output6.txt"

    tableEnv.connect( new FileSystem().path(outputPath)) // 定义表数据来源，外部连接
      .withFormat(new Csv())  //定义从外部系统读取之后的格式化方法
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
//        .field("temperature",DataTypes.DOUBLE())
        .field("cnt",DataTypes.BIGINT())   // 不能使用count命名字段注册表，因为count是sql中的函数，会有歧义
      )  //定义表结构
      .createTemporaryTable("outputTable") //创建临时表

    // 4.2 输出到文件中
//    resultTable.insertInto("outputTable")
//    aggTable.insertInto("outputTable")  //error ，AppendStreamTableSink requires that Table has only insert changes.


    // 5.执行任务
    env.execute("Test04_FileOutput")
  }

}
