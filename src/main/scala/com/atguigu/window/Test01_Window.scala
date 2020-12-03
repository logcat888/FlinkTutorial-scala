package com.atguigu.window

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author chenhuiup
 * @create 2020-11-15 16:17
 */
/*
窗口分配器：
1. window的参数列表为 窗口分配器WindowAssigner[_ >: T, W]，分配器主要是时间窗口比如滚动窗口，滑动窗口，会话窗口，
    如果想使用计算窗口就直接使用.countWindow（）。
def window[W <: Window](assigner: WindowAssigner[_ >: T, W]): WindowedStream[T, K, W] = {
    new WindowedStream(new WindowedJavaStream[T, K, W](javaStream, assigner))
  }
2. 对于不同的时间语义有不同的定义方式。
    TumblingEventTimeWindows  的构造器受保护，只能通过of方法创建
    of方法有两个重载的方法，有或没有offset，offset注意是同步不同时区时间使用，详看源码解释
3. timeWindow底层还是调用window，只不过是一种简写的方式。
    This is a shortcut for either `.window(TumblingEventTimeWindows.of(size))`
4. countWindow底层是通过GlobalWindows实现
5. 做窗口操作时，有节文本流的bug，读取完文件后，没有到达窗口时间，就会直接退出程序，不会有任何输出，
    因此为了演示效果可以改变为socketTextStream，或者kafka。
6. 在Flink底层在窗口内只有数据到来时才会创建桶放入数据，如果窗口内没有数据到来，就根本不会创建bucket。
 */
object Test01_Window {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    // 2.读取数据
//    val inputDataStream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputDataStream = env.socketTextStream("hadoop102",7777)

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
  val resultStream = dataStream.map(
      data => (data.id, data.temperature,data.timestamp))
      .keyBy(_._1) //按照二元组的第一个元素(id)分组
      // 注意：导包import org.apache.flink.streaming.api.windowing.time.Time
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口
//        .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3)))  //滚动时间窗口
//        .window(EventTimeSessionWindows.withGap(Time.seconds(10))) //会话窗口
//      .countWindow(10)  //滚动计数窗口
            .timeWindow(Time.seconds(15)) //滚动时间窗口
    .allowedLateness(Time.minutes(1)) //允许迟到的数据，设置1分钟等待
    .sideOutputLateData(new OutputTag[(String, Double, Long)]("late")) //将迟到的数据收集到侧输出流，并打上标签
    //使用reduce聚合函数，reduce算子的数据类型不能发生变化
      .reduce((curRes,newData) => (newData._1,Math.min(curRes._2,newData._2),Math.max(curRes._3,newData._3)))


    // 4.任务执行
    env.execute("Test01_Window")
  }
}

/*

  def countWindow(size: Long, slide: Long): WindowedStream[T, K, GlobalWindow] = {
    new WindowedStream(javaStream.countWindow(size, slide))
  }
  注：GlobalWindows为全局窗口，将来的所有数据都扔到一个窗口中没有结束时间，必须自定义什么时候触发
      trigger触发器
      evictor移除器，什么样的数不做考虑
  	public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
		return window(GlobalWindows.create())
				.evictor(CountEvictor.of(size))
				.trigger(CountTrigger.of(slide));
	}

 */

class MyReducer extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t1.timestamp,t1.temperature.min(t.temperature))
  }
}
