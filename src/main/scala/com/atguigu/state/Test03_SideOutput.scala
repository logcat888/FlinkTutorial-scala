package com.atguigu.state

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author chenhuiup
 * @create 2020-11-18 21:24
 */
/*
使用process API实现侧输出流
 */
object Test03_SideOutput {
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

    // 4.使用process API 实现侧输出流，类似分流操作,分出高温流和低温流
   val highTempStream =  sensorDS.process(new SplitTempProcessor(30.0))

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String,Long,Double)]("low")).print("low")

    // 4.任务执行
    env.execute("Test03_SideOutput")
  }

}
/*
实现自定义ProcessFunction，进行分流
1. ProcessFunction<I, O> 泛型为输入数据类型以及主流的输出类型
2. 使用ctx.output()，输出到侧输出流
3. new OutputTag[(String,Long,Double)]("low")给 流打上标签
 */
class SplitTempProcessor(threshold:Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature > threshold){
      // 如果当前温度值大于30，那么输出到主流
      out.collect(value)
    }else{
      // 如果不超过30度，那么输出到侧输出流
      ctx.output(new OutputTag[(String,Long,Double)]("low"),(value.id,value.timestamp,value.temperature))
    }
  }
}