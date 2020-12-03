package com.atguigu.sink

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-15 10:39
 */
object Test01_FileSink {
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

    dataStream.print()
    // 保存为csv文件，csv文件是以，进行分割。已经过时Please use the
    // [[org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink]] explicitly using the [[addSink()]] method.
//    dataStream.writeAsCsv("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\output2.txt")

    /*
    添加sink源：
    1.需要传入一个SinkFunction： def addSink(sinkFunction: SinkFunction[T]): DataStreamSink[T]
    2.StreamingFileSink的构造器 权限为受保护的，protected StreamingFileSink()
    3.通过forRowFormat（行式存储），forBulkFormat（Bulk为文件块，类似于hive中支持列式存储一样），调用build方法进行构建对象
    4.Fink是分布式框架，文件的输出是以文件夹（加上时间后缀）和part的形式输出，而不是像writeAsCSV以单个文件的形式输出。
    5.输出时调用的是toString方法。
     */
    dataStream.addSink(
      // a.调用forRowFormat获取返回值build进行构建对象。
      StreamingFileSink.forRowFormat(
        //
        new Path("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\output2.txt"),
        // b.添加序列化器，默认是UTF-8。 因为要写入文件中，当然需要一样序列化的工具。直接调用toString方法。
        new SimpleStringEncoder[SensorReading]()
        // c.调用build方法进行构建
      ).build()
    )

    // 4.执行任务
    env.execute()
  }
}
