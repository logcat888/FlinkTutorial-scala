package com.atguigu.state

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author chenhuiup
 * @create 2020-11-17 23:39
 */
/*
需求：10秒内温度连续上升，输出报警信息，使用定时器实现
 */
object Test02_ProcessFunction {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
    val inputDataStream = env.socketTextStream("hadoop102", 7777)

    // 3.转换数据结构
    val sensorDS: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    //      .keyBy(_.id)
    //      .process(new MykeyedProcessFunction)

    val warningStream = sensorDS
        .keyBy(_.id)
        .process(new TempIncreWarning(10000L))

    warningStream.print()

    // 4.任务执行
    env.execute("Test02_ProcessFunction")
  }

}

// 实现自定义的KeyedProcessFunction实现10秒内温度连续上升，输出报警信息，使用定时器实现
// interval代表监测的时间范围
class TempIncreWarning(interval:Long) extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义值状态保存上次的温度以及定时器时间戳
  // 定义状态：保存上一个温度值进行比较，保存注册定时器的时间戳用于删除
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsState",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 获取状态,并更新温度
    val lastTemp = lastTempState.value()
    val timerTs = timerTsState.value()
    lastTempState.update(value.temperature)

    // 当前温度值和上次温度进行比较
    if (value.temperature > lastTemp && timerTs == 0){
      // 当本次温度大于上次温度，且定时器时间戳为初始值时创建定时器
      // 如果温度上升，且没有定时器，那么注册当前时间10s之后的定时器
      ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + interval)
      // 更新定时器时间戳状态
      timerTsState.update(ctx.timestamp())
    }else if (value.temperature < lastTemp){
      // 如果温度下降，那么删除定时器
      ctx.timerService().deleteProcessingTimeTimer(ctx.timestamp() + interval)
      // 清空定时器时间戳的状态
      timerTsState.clear()
    }

  }
   //定时时间到的处理逻辑
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval/1000 + "秒连续上升")
    // 清空定时器时间戳的状态
    timerTsState.clear()
  }
}

/*
KeyedProcessFunction功能测试
1. KeyedProcessFunction<K, I, O> 泛型分别为key的类型，输入数据的类型，输出数据的类型
2. 使用Collector输出数据的好处就是可以在处理一条数据时，不输出数据，从而实现filter的效果
3. 可以定义多个定时器，不同定时器通过时间戳进行区分，定义了相同时间戳的定时器，会认为是一个定时器，定时时间到后会调用onTimer方法
4. 删除定时器也是通过时间戳确定
 */
class MykeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {


  var myState: ValueState[Int] = _

  // 可以定义RichFunction中的操作
  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  }

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp()
    //    ctx.output()  //侧输出流
    ctx.timerService().currentWatermark() //获取当前的Watermark
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L) //注册定时器，60s后处理
    //    ctx.timerService().deleteEventTimeTimer(ctx.timestamp() + 60000L) //删除定时器
  }

  /**
   * 定时时间到后的处理逻辑
   *
   * @param timestamp 定时时间，可以根据此时间区分不同的定时器
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

  }
}
