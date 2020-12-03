package com.atguigu.udf

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @author chenhuiup
 * @create 2020-11-22 9:25
 */
/*
自定义表聚合函数，提取所有温度值中最高的两个温度，输出（temp，rank）
1.步骤：table API
  1）定义一个类继承TableAggregateFunction<T, ACC> 泛型解读，T为输出数据类型，ACC为聚合状态的类型
  2）实现一个方法createAccumulator（）创建状态
  3）实现计算聚合结果的函数accumulate，方法名必须为accumulate，参数列表分别为聚合状态,输入数据的类型
  4）实现一个输出结果的方法，最终处理完表中所有数据时调用方法名必须为emitValue，参数列表分别为聚合状态，收集器，返回值类型必须是Unit。收集器是org.apache.flink.util.Collector包下的
  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
  5）定义一个类，用来表示表聚合函数的状态
2.使用table API实现表聚合函数实现TopN的需要，可以定义一个TreeMap的结构实现排序插入，如果使用案例中定义多个变量实现TopN的判断逻辑就比较复杂
3.由于是聚合后更新操作，而不是更新操作，所以必须使用toRetractStream进行输出
4. sql 方式不好使用表聚合函数实现topN，但是SQL中有Rank函数也能实现同样的效果
5. 使用process API也同样可以实现TopN，一般情况不使用table API 实现TopN
 */
object Test04_TableAggregateFunction {
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
    // a.创建TableAggregateFunction对象
    val top2Temp = new Top2Temp()
    // b.
    val resultTable = sensorTable.groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank)) // 指定炸裂后的字段
      .select('id, 'temp, 'rank)

    // 6.sql 方式不好使用表聚合函数实现topN，但是SQL中有Rank函数也能实现同样的效果

    // 7.打印
    resultTable.toRetractStream[Row].print("result")

    /*
    result> (false,sensor_1,35.8,1)
    result> (false,sensor_1,-1.7976931348623157E308,2)
    result> (true,sensor_1,35.8,1)
    result> (true,sensor_1,32.0,2)
    result> (false,sensor_1,35.8,1)
    result> (false,sensor_1,32.0,2)
    result> (true,sensor_1,36.2,1)
    result> (true,sensor_1,35.8,2)
     */

    // 8.执行任务
    env.execute()
  }

}

// 定义一个类，用来表示表聚合函数的状态
class Top2TempAcc{
  var highestTemp:Double =Double.MinValue
  var secondHighestTemp:Double =Double.MinValue
}

//自定义表聚合函数，提取所有温度值中最高的两个温度，输出（temp，rank）
// TableAggregateFunction<T, ACC> 泛型解读，T为输出数据类型，ACC为聚合状态的类型
class Top2Temp extends TableAggregateFunction[(Double,Int),Top2TempAcc]{
  // 创建状态
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  //实现计算聚合结果的函数accumulate
  // 方法名必须为accumulate，参数列表分别为聚合状态,输入数据的类型
  def accumulate(acc:Top2TempAcc,temp:Double):Unit={
    // 判断当前温度值，是否比状态中的值大
    if (temp > acc.highestTemp){
       // 如果比最高温度还高，排在第一，原来的第一顺到第二
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp=temp
    }else if (temp >acc.secondHighestTemp){
      // 如果小于最高大于第二，则替换第二
      acc.secondHighestTemp = temp
    }
  }

  // 实现一个输出结果的方法，最终处理完表中所有数据时调用
  // 方法名必须为emitValue，参数列表分别为聚合状态，收集器，返回值类型必须是Unit。收集器是org.apache.flink.util.Collector包下的
  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
    out.collect((acc.highestTemp,1))
    out.collect((acc.secondHighestTemp,2))

  }
}