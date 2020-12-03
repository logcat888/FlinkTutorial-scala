package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-09 7:31
 */
/*
1. 从集合/文件中读取数据，相当于批处理，有界流处理，当集合/文件中的数据处理完后程序就退出。
2. 在本地执行，并行度默认是CPU核心数，所以输出时会出现乱序，没有和集合中元素的顺序一致
3. 尽管只进行了输入和输出，没有进行转换，但是依然需要引入隐式转换。
4. 手动导包import org.apache.flink.streaming.api.scala._
5.  fromElements是读取任何类型的数据，只是测试时使用，没有必要将类型设置为不同，生产中数据都是经过ETL清洗的，类型都一致。
 */
object Test01_SourceTest {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.从集合中读取数据
    val dataList: List[SensorReading] = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
    // fromElements是读取任何类型的数据，只是测试时使用，没有必要将类型设置为不同，生产中数据都是经过ETL清洗的，类型都一致。
//    env.fromElements(1.0,35,"hello",true)

    // 2.从文件中读取数据，也相当于批处理
    val inputPath = "D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    // 3.打印
    stream1.print()
    stream2.print()

    // 4.执行
    env.execute("source test")
  }
}

// 定义样例类，网联网的温度传感器，实时监控温度变化
case class SensorReading(id:String,timestamp:Long,temperature:Double)
