package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-09 23:59
 */
/*
多流转换操作--分流操作
需求：将传感器温度数据分成低温和高温两个流
 */
object Test06_split_select {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
    val inputDataStream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    // a.使用split将一个DataStream，分组打标签，split算子已经过时，推荐使用side output侧流算子
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    // b.使用select拣选不同标签的流，支持选择多个标签，从而完成流的切分
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high","low")

    // 4. 打印流，给流贴上标签
    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")

    // 5.执行任务
    // jobName也可以不传
    env.execute("split")
  }

}
