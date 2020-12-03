package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-08 10:02
 */
/*
流处理的word count
1. 手动导入import org.apache.flink.streaming.api.scala._  ,以便能够使用scala中定义的隐式转换
2. 流处理的执行环境也是允许读取文件中数据。
3. 批处理使用的是DataSetAPI，流处理使用的是DataStreamAPI
4. 流处理中没有groupBy，但是提供了keyBy，支持提取key，字段索引等.
5. 流处理需要事件驱动，只有数据到来，才执行计算。
6. flink是分布式流处理框架，所以默认是并行执行的，输出结果前的数字，表示计算当前结果的任务到底运行在哪个并行的子任务中。
  如果没有指定并行任务个数，那么并行任务的个数默认取决于当前机器的CPU核数。
7. 如果当前CPU核数是4个，也可以执行8个线程，因为单CPU可以采用时间片的策略，开启多线程。
8. keyBy分区，默认根据word的hash值分配计算到底出现哪个线程，类似于Shuffle，但是不需要等待。
9. 如何设置并行度？
  1）全局设置：指定并行度env.setParallelism(8)
  2）默认设置：如果没有全局设置并行度，默认是CPU核心数。
  3)算子设置：在flink中算子之间是独立的，可以为每个算子设置并行度。一个数据经过一个算子计算后可以流向下一个算子，之间没有延迟。
            因此每个算子可以有不同并行数。但是一般不对算子设置并行度，因为既然集群有资源，在全局设置一下并行度即可。但是也有
            应用场景，比如设置一个并行度输入到文件。
10. 从外部命令中提取参数
    1）mian方法传入参数，Flink提供一个工具类包装main方法中的参数，本质还是main方法传入参数
    2）使用配置文件传入参数

11. 分布式架构乱序问题：
    在wordcount中，可以发现同一行数据（hello flink）在最后输出时可能flink在前，hello在后，这是由于经过分布式算子操作时，
    由于传输时不同word的网络距离不同，导致的乱序。可以设置并行度为1，最终的输出结果就是按照顺序输出，但是这样设置就没有大数据
    并行计算的意义。

 */
object Test02_StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度,开启8个线程执行任务
    env.setParallelism(8)

    // 6.从外部命令中提取参数，避免写死，作为socket主机名和端口号
    // 6.1 将mian方法中的参数包装成ParameterTool
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    // 6.2 按照顺序读取参数
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    // 2.接收一个socket文本流
    //    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
    // 3.进行转化处理统计
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1)).setParallelism(3)
      //      .keyBy(_._1)
      .keyBy(0)
      .sum(1).setParallelism(2)

    // 4.打印输出
    resultDataStream.print()
    //    resultDataStream.print().setParallelism(1) 设置print算子的并行度是1，就可以在一个线程中执行打印。
    // 比如在写入文件时，为了保证顺序以及避免多个线程同时操作，就可以设置并行度为1

    // 5.启动任务执行，等待数据源源不断的输入，如果没有启动主线程就结束了
    // 可以在execute中指定流式处理程序的名称，当然也可以不指定
    //    env.execute()
    env.execute("stream word count")

  }

}
