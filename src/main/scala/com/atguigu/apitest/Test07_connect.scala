package com.atguigu.apitest

import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-10 8:02
 */
/*
合流算子：connect和map  ,  union
合流的应用场景：
比如风险控制做报警，温度传感器记录环境温度，但是只报警高温可能不准确，也有可能是太阳直射传感器导致温度升高；这时候再搭配上
烟雾报警器，当既有高温数据又有烟雾数据即可表示火情的预警。
1.合流前两条流分别在不同的slot上执行，当然也会有不同的任务在同一个slot中共享，所以两条流也可以在同一个slot中，
  可以理解为两条流分别是前后发生的不同的任务。再经过coMap后合成一个流，当然在合流前每个流都可以有并行任务，且
  合流后也可以并行任务，可以分布在不同的slot中。
2.可以将不同数据类型的流合并为一个流ConnectedStreams[IN1, IN2]，对合流后的流进行处理时需要传入不同的处理函数，
  操作完后又回到DataStream[T]，由于任何类型都是Any的子类，所以合并转换后的流的数据类型肯定一致。
3.union可以合并多个流，但是数据类型必须一致。
 */
object Test07_connect {
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
    // a.使用split将一个DataStream，分组打标签，split算子已经过时，推荐使用side output侧流算子
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    // b.使用select拣选不同标签的流，支持选择多个标签，从而完成流的切分
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    // 4. 合流
    //转变数据类型，为了演示合流可以包含不同的类型
    val warningStream = highTempStream.map( data => (data.id,data.temperature))
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    // 5.用coMap对数据进行分别处理
    //尽管一个流是二元组，一个流是三元组，但是由于元组统一的类型都是Product类型，所以输出流依然是同一个类型,将类型写为Any也行
    val coMapResultStream: DataStream[Product with Serializable] = connectedStreams
      .map(
        //三元组类型
        warningData => (warningData._1, warningData._2, "warning"),
        //二元组类型
        lowTempData => (lowTempData.id, "healthy")
      )
    coMapResultStream.print("coMap")
    /*
    coMap> (sensor_1,35.8,warning)
    coMap> (sensor_6,healthy)
    coMap> (sensor_10,38.1,warning)
    coMap> (sensor_7,healthy)
    coMap> (sensor_1,32.0,warning)
    coMap> (sensor_1,healthy)
    coMap> (sensor_1,36.2,warning)
    coMap> (sensor_1,30.9,warning)
     */

    // 6.union合流
//    warningStream.union(lowTempStream) // error,类型不匹配
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream,allTempStream) //ok
    

    // 5.执行任务
    // jobName也可以不传
    env.execute("split")
  }
}

/*
注：泛型代表数据流1和数据流2，说明包含两个流
class ConnectedStreams[IN1, IN2](javaStream: JavaCStream[IN1, IN2])

注：ConnectedStreams的map算子，匿名函数的返回值都相同，分别传入对两个不同流的处理函数
  def map[R: TypeInformation](fun1: IN1 => R, fun2: IN2 => R):
      DataStream[R] = {

    if (fun1 == null || fun2 == null) {
      throw new NullPointerException("Map function must not be null.")
    }
    val cleanFun1 = clean(fun1)
    val cleanFun2 = clean(fun2)
    val comapper = new CoMapFunction[IN1, IN2, R] {
      def map1(in1: IN1): R = cleanFun1(in1)
      def map2(in2: IN2): R = cleanFun2(in2)
    }

    map(comapper)
  }

 注：map算子的重载，形参为函数类，因为是CoMap，所以将合流的map称为coMap
 def map[R: TypeInformation](coMapper: CoMapFunction[IN1, IN2, R]): DataStream[R]

 注：CoMapFunction的泛型分别为数据流1.数据流2，输出流。
 public interface CoMapFunction<IN1, IN2, OUT> extends Function, Serializable {
    //定义两个抽象方法map1，map2，返回值类型为OUT
    OUT map1(IN1 var1) throws Exception;

    OUT map2(IN2 var1) throws Exception;
}

 */