package com.atguigu.state

import java.util.concurrent.TimeUnit

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-19 8:23
 */
/*
状态后端、检查点配置及重启策略配置
一.状态后端的设置：

二.检查点的配置：
1.配置检查点之前先启用检查点，flink默认是禁用检查点的。
  1)env.enableCheckpointing() 空参构造已经过时，默认是500ms生成检查点。
  2）时间间隔：checkpoint设置的时间间隔不能过长，否则容错恢复的时间就长，影响实时性。也不能过短，watermark默认是200ms，数据保存应该比数据读取的标记大一些。
  3）检查点模式：Checkpointing默认的模式是精准一次性，此外还有至少一次（如果为了追求实时性，对正确性要求不高，就可以将检查点模式的级别调低）。
    def enableCheckpointing(interval : Long) : StreamExecutionEnvironment = {
    enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE)
  }
  4）超时时间：设置检查点超时时间（一般是分钟级别），jobmanager向source发送设置检查点指令，各个节点完成检查点后会给jobmanager一个完成回信，
    当回信时间超时时就丢弃该检查点的设置。
  5）异步快照：做状态点保存时，并不意味着该任务需要暂停，而是检查点的保存和状态的更新可以同时进行（asynchronousSnapshots异步快照），
    做状态保存中最耗时的就是向远程同步状态，当需要做检查点时该任务会在内存中复制一份状态，同时更新状态可以正常进行。
    同步快照：是指必须等状态保存完之后才能继续计算，如果一秒钟触发一次，一次保存就需要60秒，相当于不能干活了。
      public FsStateBackend(String checkpointDataUri, boolean asynchronousSnapshots) {
        this(new Path(checkpointDataUri), asynchronousSnapshots);
    }
    6）设置最大并行几个检查点保存，默认是一个。env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    7）设置两次检查点最小时间间隔（即向流中插入barrier的间隔）。检查点时间间隔指的是两个检查点头与头之间的间隔，最小时间间隔
        指的是上一个检查点的尾与下一个检查点的头之间的间隔。env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    8）如果配置了最小时间间隔，那么就会覆盖最大并行度，即便设置了并行度，也只能是1。
    9）设置优先使用checkpoint进行故障恢复，env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    10）最大容忍几次checkpoint保存失败。 env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    11）一般在生产环境中，配置时间间隔、检查点模式、超时时间、最小时间间隔

三、重启策略配置
    1)fixedDelayRestart：固定时间间隔重启：参数解读：1）重启次数；2）每次重启的时间间隔
    2)failureRateRestart:失败率重启：1）failureRate，重启次数, 2）failureInterval：时间段, 3）delayInterval每次重启的时间间隔
    3)fallBackRestart：回滚，不做处理，如果上级有处理机制就交给上级，flink不做处理。一般不用
    4）noRestart: 不重启。一般不用
 */
object Test04_checkpoint {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

    //设置状态后端
//    env.setStateBackend(new MemoryStateBackend()) //默认级别就是内存级别
//    env.setStateBackend(new FsStateBackend("路径")) //
//    env.setStateBackend(new RocksDBStateBackend("路径"))

    // a1.启用检查点
    env.enableCheckpointing(1000L)
    // a2.设置检查点模式,对正确性要求不高，追求实时性，追求性能，可以设置 至少一次的检查点级别
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    // a3.设置检查点超时时间（一般是分钟级别），jobmanager向source发送设置检查点指令，各个节点完成检查点后会给jobmanager一个完成回信，
    // 当回信时间超时时就丢弃该检查点的设置
    env.getCheckpointConfig.setCheckpointTimeout(60*1000L)
    // a4.设置最大并行几个检查点的保存，默认是一个。如果设置成2个，就代表着前一个检查点没有保存完成，下一个检查点的保存可以进行。
    // 检查点的保存与一个任务接受到barrier后自己状态的保存是两回事，检查点是将所有任务的状态进行合并形成的，一个任务的状态保存
    // 可能很快，但是一个检查点的保存需要所有任务都完成才能算完成，因此检查点的保存就慢一些。如果使用默认1个并行，当后面的任务
    // 没有还完成状态的保存，而jobmanager向source发生检查点的时间到了，这时source不能进行检查点的保存，必须等到后面的状态保存完
    // 之后才能将下一个检查点加入到流中，进行状态的保存。如果设置多个并行，及时后面的任务没有完成，也可以进行状态保存，不能暂停
    // 任务。设置最大并行的目的就是避免性能都用于检查点的保存，而没有时间进行计算。
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    // a5.设置两次检查点最小时间间隔（即向流中插入barrier的间隔）。检查点时间间隔指的是两个检查点头与头之间的间隔，最小时间间隔
    // 指的是上一个检查点的尾与下一个检查点的头之间的间隔。 如果检查点时间间隔设置为1秒，最小时间间隔设置为500ms，且没有启动并行度，
    // 一个检查点的保存需要1.5秒，当时间到了2秒时也不能在流中插入barrier，必须等到检查点保存完，且过了500ms才能在流中插入barrier，
    // 即在2秒时候才插入barrier。所以如果配置了最小时间间隔，那么就会覆盖最大并行度，即便设置了并行度，也只能是1。
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    // a6.设置优先使用checkpoint进行故障恢复，默认是false。如果设置了true，就意味着即使手动保存的savepoint的时间比checkpoint新，
    // 也只用checkpoint进行故障恢复。
    env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    // a7.最大容忍几次checkpoint保存失败。如果设置为0，即使计算正常进行，而checkpoint保存失败，也会认为任务失败，会进行任务的重启。
    // 如果设置了3，即使check保存失败，但是任务的计算还在正常进行，也不会认为任务失败，且会最大容忍3次失败。
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // b、重启策略配置
    // b1.fixedDelayRestart固定时间间隔 参数解读：1）重启次数；2）每次重启的时间间隔
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10*1000L))
    // b2.failureRateRestart参数解读：1）failureRate，重启次数, 2）failureInterval：时间段, 3）delayInterval每次重启的时间间隔
    // 意思是在 5分钟内重启3次，每次时间间隔为30秒。
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.of(5,TimeUnit.MINUTES),Time.seconds(30)))

    // 2.读取数据
    val inputDataStream = env.socketTextStream("hadoop102", 7777)

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 需求：对于温度传感器温度值跳变，超过10度，报警
    val alertStream =dataStream
      .keyBy(_.id)
      //      .flatMap(new TempChangeAlert(10.1))  //使用自定义RichFlatMapFunction实现状态的管理
      .flatMapWithState[(String,Double,Double),Double]({ //必须指定flatMapWithState的泛型，否则scala无法更加函数体推测泛型
        case (data:SensorReading,None) => (List.empty,Some(data.temperature))
        case (data:SensorReading,lastTemp:Some[Double]) =>{
          // 计算两次温度是否超过阈值
          val diff = (lastTemp.get - data.temperature).abs
          if (diff > 10.0)
          // 如果温度超过阈值，输出结果可遍历的集合，并更新状态
            (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
          else
          // 如果温度不超过阈值，输出结果为空集合（不输出），并更新状态
            (List.empty,Some(data.temperature))
        }
      })

    //处理结果数据
    alertStream.print("result")
    // 4.任务执行
    env.execute("Test01_Window")
  }

}
