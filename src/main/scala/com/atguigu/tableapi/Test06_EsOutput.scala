package com.atguigu.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

/**
 * @author chenhuiup
 * @create 2020-11-19 21:33
 */
/*
将数据写出到Es中：
1. 将聚合后的表写入到Es中，聚合字段就是Es中的doc_id，因此在流式数据到来后，可以找到doc_id进行更新。
2. 指定模式，es支持更新插入模式，如果外部系统不支持运行时会报错
3. Es写入数据时支持Map结构或Json字符串，这里必须写入Json字符串，且需要引入依赖
4. 使用curl "hadoop102:9200/sensor/_search?pretty"
5. ElasticsearchUpsertTableSinkBase实现了UpsertStreamTableSink，所以Es支持inUpsertMode模式，但是Es不支持Retract模式
public abstract class ElasticsearchUpsertTableSinkBase implements UpsertStreamTableSink<Row> {
6. 在ElasticsearchUpsertTableSinkFactoryBase源码210中写到Es不支持Retract模式，既然支持UpsertMode，就没有必要支持Retract模式。
 */
object Test06_EsOutput {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1.创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //  2.连接外部系统，读取数据，注册表
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

    // 4 输出到Es
    tableEnv.connect(new Elasticsearch()
    .version("6") // 指定Es的版本
    .host("hadoop102",9200,"http")
        .index("sensor01")
        .documentType("temperature")
    )
      .inUpsertMode() // 指定模式，es支持更新插入模式，如果外部系统不支持运行时会报错
      .withFormat( new Json()) // Es写入数据时支持Map结构或Json字符串，这里必须写入Json字符串，且需要引入依赖
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("count",DataTypes.BIGINT())
      ).createTemporaryTable("esOutputTable")

    // 将聚合后的表插入到Es中
    aggTable.insertInto("esOutputTable")

    // 5.执行
    env.execute("Test06_EsOutput")
  }

}
