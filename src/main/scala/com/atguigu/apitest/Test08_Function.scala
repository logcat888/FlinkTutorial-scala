package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author chenhuiup
 * @create 2020-11-15 10:04
 */
/*
函数类与富函数
 */
object Test08_Function {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //3.

    //4.执行任务
    env.execute("Test08_Function")
  }
}

//自定义一个函数类
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = t.id.startsWith("sensor_1")
}

class MyMap extends MapFunction[SensorReading,String]{
  override def map(t: SensorReading): String = t.id + "传感器"
}

/*
富函数，
1. 可以获取运行时上下文进行读写操作（getRuntimeContext，setRuntimeContext）
2. 还有一些生命周期方法（open、close），做一些初始化操作以及收尾工作，比如数据库的连接，以及获取状态进行状态编程。
 */
class MyRichMap extends RichMapFunction[SensorReading,String] {

  override def open(parameters: Configuration): Unit = {
    // 0.做一些初始化操作，比如数据库的连接，
    // 1.如果获取连接是方法连接放在map方法中相当于每来一个数据建立一次连接，效率低
    // 2.open什么时候被调用？open是在函数对象被创建好后，加入到Flink的运行时环境中后，数据还没有到来，但是任务已经准备好后被调用。
    //  因此open获取运行时上下文（getRuntimeContext()）只能在方法内，而不能放在方法外。
//    getRuntimeContext()

  }

  override def close(): Unit = {
    // 1.一般做收尾工作，比如关闭连接，或者清空状态
    // 2.状态保存在运行时上下文中，通过获取状态进行状态编程。
  }

  override def map(in: SensorReading): String = in.id + "传感器"
}