package com.atguigu.apitest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @author chenhuiup
 * @create 2020-11-09 20:36
 */
/*
自定义source

 */
object Test03_SourceTest {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.从自定义source中读取数据
    val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

    // 3.打印
    dataStream.print()

    // 4.执行任务
    env.execute()

  }
}
/*
1. 实现SourceFunction[T]，泛型为发送数据的类型
2. SourceContext中有个collect方法，负责发送数据
collect[T]: 收集要发送的数据，通过collect发送数据，发送数据的类型就是T类型
3. run方法的形参为上下文对象，上下文中有个collect方法负责发送数据。
4. cancel:停止发送数据，需要使用标志位停止
 */
//自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{

  //定义一个标志位flag，用来表示数据源是否正常运行发送数据
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 定义一个随机数发生器
    val random = new Random()

    // 随机生成一组（10个）传感器的初始温度：（id，temp）
    var curTemp = (1 to 10) map(i => ("sensor_" + i,random.nextDouble() * 100))

    // 定义无限循环，不停地产生数据，除非被cancel
    while (running){
      //在上次数据基础上微调，更新温度值
      curTemp.map(
         // nextGaussian,高斯分布（正态分布，μ=0，σ=1）
        data => (data._1, data._2 + random.nextGaussian())
      )

      // 获取当前时间戳，加入到数据中,调用sourceContext.collect（）发送数据
      val curTime = System.currentTimeMillis()
      curTemp.foreach(data => sourceContext.collect(SensorReading(data._1,curTime,data._2)))

      // 间隔100ms,发送一次
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = running=false
}

