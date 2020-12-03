package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-09 22:32
 */
/*
1. min
1）当并行度没有指定时，并行度为cpu核心数，在map算子时数据是并行的，所以进入keyBy的数据的先后顺序可能不同
2）min的参数可以指定字段，也可以指定索引
3）min求最小值时，尽管指定字段会得到最小值，但是其他字段会是第一个进入slot的key对应的字段，之后都会使用这个值

2.minBy：求出最小值，且其他字段与使用数据自己的值
 */
object Test04_TransformTest {
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
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id") //根据id进行分组，也可以传入索引
//      .min("temperature") //根据字段进行求最小值，也可以传入索引
        .minBy("temperature")
    // 4. 打印
    aggStream.print()

    // 5.执行任务
    // jobName也可以不传
    env.execute("aaa")
  }

}
