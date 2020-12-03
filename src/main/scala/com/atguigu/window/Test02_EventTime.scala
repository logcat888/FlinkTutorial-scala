package com.atguigu.window

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author chenhuiup
 * @create 2020-11-16 7:18
 */
/*
时间语义：
1.Watermark就是一个类，定义了一种数据结构，只有时间戳
2.BoundedOutOfOrdernessTimestampExtractor解读：
  1）参数列表为：用户定义的最大乱序程度
  2）重写extractTimestamp：从数据中提取时间戳，必须是毫秒数。
3.默认的周期性生成Watermark的时间周期是200ms，当然可以设置env.getConfig.setAutoWatermarkInterval(500)
4.周期性生成Watermark设置大了，影响实时性，但是结果准确性提高；设置小了，实时性提高，消耗性能也提高，结果准确性下降。
5.处理乱序数据三重保证：
    1）先设置比较小的Watermark延迟时间，快速得到一个近似正确的结果，且能够包含大部分延迟的数据；
    2）如果要求更加精准，就可以设置处理迟到数据；
    3）最后数据不能一直等待下去，可以设置输出到侧输出流中，保证数据不丢。
 */
object Test02_EventTime {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)
    // 设置事件时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置周期性WatermarkInterval的时间间隔为500ms
    env.getConfig.setAutoWatermarkInterval(500)

    // 2.读取数据
    val inputDataStream = env.socketTextStream("hadoop102",7777)

    //设置标签
    val latetag = new OutputTag[(String, Double, Long)]("late")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
//      .assignAscendingTimestamps(_.timestamp * 1000L) //升序数据提取时间戳，不用定义Watermark
      //Watermark引入
      // Flink提供好的对于乱序数据提取时间的周期性生成Watermark的类，BoundedOutOfOrdernessTimestampExtractor：有界乱序时间戳提取器。
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 每15秒统计一次，窗口内各传感器所有温度的最小值，以及最新的时间戳
    val resultStream = dataStream.map(
      data => (data.id, data.temperature,data.timestamp))
      .keyBy(_._1) //按照二元组的第一个元素(id)分组
      .timeWindow(Time.seconds(15)) //滚动时间窗口
      .allowedLateness(Time.minutes(1)) //允许迟到的数据，设置1分钟等待
      .sideOutputLateData(latetag) //将迟到的数据收集到侧输出流，并打上标签
      //使用reduce聚合函数，reduce算子的数据类型不能发生变化
      .reduce((curRes,newData) => (newData._1,Math.min(curRes._2,newData._2),Math.max(curRes._3,newData._3)))

    //处理侧输出流数据
    resultStream.getSideOutput(latetag).print("late-哈哈哈")
    //处理结果数据
    resultStream.print("result")
    // 4.任务执行
    env.execute("Test01_Window")
  }
}

/*
注：Watermark就是一个类，定义了一种数据结构，只有时间戳
public final class Watermark extends StreamElement {

	 //The watermark that signifies end-of-event-time.
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

  //The timestamp of the watermark in milliseconds.
	private final long timestamp;

	//Creates a new watermark with the given timestamp in milliseconds.
	public Watermark(long timestamp) {
		this.timestamp = timestamp;
	}

注：
1. 时间语义对于默认的ProcessingTime的WatermarkInterval是0，
2. 时间语义为事件时间时，默认的WatermarkInterval是200毫秒。

	public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
		this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
		if (characteristic == TimeCharacteristic.ProcessingTime) {
			getConfig().setAutoWatermarkInterval(0);
		} else {
			getConfig().setAutoWatermarkInterval(200);
		}
	}
 */

