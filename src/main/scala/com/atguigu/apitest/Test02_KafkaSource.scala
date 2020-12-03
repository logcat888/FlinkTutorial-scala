package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @author chenhuiup
 * @create 2020-11-09 19:34
 */
/*
从kafka中读取数据
1. addSource有两个重载的方法，SourceFunction[T]是一个继承函数的接口，需要类似于函数功能的函数类。
def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
2. 需要引入kafka-flink的依赖
3. FlinkKafkaConsumer011继承于10，而10继承于9,而9继承于FlinkKafkaConsumerBase, 他们都实现了SourceFunction。
4. FlinkKafkaConsumer011参数解读：泛型为消息的value，第一个参数是消费者主题，第二个参数String类型的反序列化器，
    第三个参数是kafka的配置参数
5. kafka连接参数的设置：只需要指定集群地址以及消费者组即可
    1）其实可以不用定义key和value的反序列化器，因为在消费者的构造器中只读value，且设置了反序列化器。
    2）其实offset重置设置也不必要，因为状态一致性，在底层flink-kafka的连接器会自动帮助维护提交offset。
6. 当没有指定并行度时，flink-kafka连接器会自动检测消费的topic的分区数，根据分区数设置并行度。
   当topic只有一个分区时，并行度就是1，只有一个source从topic消费数据。
 */
object Test02_KafkaSource {
  def main(args: Array[String]): Unit = {

    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 从kafka中读取数据
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"uzi")
    // 其实可以不用定义key和value的反序列化器，因为在消费者的构造器中只读value，且设置了反序列化器。
//    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
//    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    // 其实offset重置设置也不必要，因为状态一致性，在底层flink-kafka的连接器会自动帮助维护提交offset。
//    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    /*
      FlinkKafkaConsumer011参数解读：泛型为消息的value，第一个参数是消费者主题，第二个参数String类型的反序列化器，第三个参数是kafka的配置参数
     */
    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),props))

    // 3.打印
    kafkaDataStream.print()

    // 4.开启任务
    env.execute("KafkaSource")

  }
}

/*
  注：添加source的两个重载方法
   * Create a DataStream using a user defined source function for arbitrary
   * source functionality. By default sources have a parallelism of 1.
   * To enable parallel execution, the user defined source should implement
   * ParallelSourceFunction or extend RichParallelSourceFunction.
   * In these cases the resulting source will have the parallelism of the environment.
   * To change this afterwards call DataStreamSource.setParallelism(int)
   注：SourceFunction[T]是一个继承函数的接口。类似于函数功能的函数类。
  def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    require(function != null, "Function must not be null.")

    val cleanFun = scalaClean(function)
    val typeInfo = implicitly[TypeInformation[T]]
    asScalaStream(javaEnv.addSource(cleanFun, typeInfo))
  }

  public interface SourceFunction<T> extends Function, Serializable {
    void run(SourceFunction.SourceContext<T> var1) throws Exception;

   * Create a DataStream using a user defined source function for arbitrary
   * source functionality.

   注：参数为匿名函数，比较难写
  def addSource[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    require(function != null, "Function must not be null.")
    val sourceFunction = new SourceFunction[T] {
      val cleanFun = scalaClean(function)
      override def run(ctx: SourceContext[T]) {
        cleanFun(ctx)
      }
      override def cancel() = {}
    }
    addSource(sourceFunction)
  }
 */
