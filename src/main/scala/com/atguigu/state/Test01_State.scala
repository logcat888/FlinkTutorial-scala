package com.atguigu.state

import java.{lang, util}

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author chenhuiup
 * @create 2020-11-17 7:54
 */
/*
测试状态
 */
object Test01_State {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度是1
    env.setParallelism(1)

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
    dataStream.print("result")
    // 4.任务执行
    env.execute("Test01_Window")
  }

}

/*
flatMapWithState解读：
1. 一般情况下，对于简单的算子（map、flatMap，filter）是不需要状态的，但是也可以通过继承RichFunction设置状态。
2. Flink对于flatMap直接提供了有状态的算子
3. 参数解读：
  1）T代表输入数据的泛型，R代表输出数据的泛型，S代表状态的泛型
  2）Flink将状态包装成Option类，None代表没有，Some代表有状态，因此可以根据是否为None判断是否是初始值
  3）函数fun的返回值是二元组类型，TraversableOnce[R]是结果，为可遍历的集合；Option[S]是状态；
  4）flatMap比map的好处就是可以不输出结果，可以通过List.empty表示空集合。
  5）必须指定flatMapWithState的泛型，否则scala无法更加函数体推测泛型
4.flatMapWithState只能在keyedStream中使用，因为flatMapWithState中的状态也是键控状态。
  def flatMapWithState[R: TypeInformation, S: TypeInformation](
        fun: (T, Option[S]) => (TraversableOnce[R], Option[S])): DataStream[R]
 */

/*
 实现自定义RichFlatMapFunction
1. FlatMap的好处就是用收集器收集结果，且可以不输出结果
2. RichFlatMapFunction[in,out] 输入数据类型，输出数据类型 ，设置一个三元组（传感器id，上次温度值，本次温度值）
3. 存在的bug，定义状态时如果没有给初始值，默认是对应类型的初始值，这样如果第一条数据是35度，那么就会直接报警，比如（sensor_1,0.0,35.0）
    解决方式一：设置初始值时，给一个77777，计算温差差值时先进行判断，如果等于77777，就不输出，只更新状态。
    解决方式二：使用flatMapWithState
 */

class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)] {
  // 定义值状态保存上次传感器的温度
  lazy val value: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("alterTemp",classOf[Double]))

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = value.value()
    // 计算两次温度是否超过阈值
    val diff = (lastTemp - in.temperature).abs
    if (diff > threshold)
      collector.collect((in.id,lastTemp,in.temperature))
    //更新状态
    value.update(in.temperature)
  }
}

/*
1. Keyed State测试必须定义在RichFunction中，因为需要运行时上下文
2. getRuntimeContext不能乱调用，不能在类构造的时候调用（即方法外），这样是拿不到运行时上下文的，因为这是一个任务，
    必须在生命周期open中才能获取到，如果在生命周期外调用是不生效的。
3. 如何定义值状态？在生命周期open中用运行时上下文获取状态局部，定义好名称和类型即可。
4. 不同状态的状态名不能重复，因为Flink就是通过状态名在slot上进行状态管理
5. 定义状态时如果没有给初始值，默认是对应类型的初始值

 */
class MyRichMapper extends RichMapFunction[SensorReading, String] {

  //a.定义值状态
  var valueState: ValueState[Double] = _

  //b.定义列表状态
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("listState", classOf[Int]))

  //c.定义map状态
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))

  //d.定义聚合状态
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
    "reduceState", //定义状态名
    new ReduceFunction[SensorReading] {
      override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.max(t1.temperature))
      }
    }, //定义ReduceFunction
    classOf[SensorReading] //定义Reduce聚合
  ))

  //error,这样获取运行时上下文失效
  //  val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

  //ok,使用lazy关键字延迟加载，也是可以获取状态的,因为是在真正执行时才被执行
  //lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

  override def open(parameters: Configuration): Unit = {
    //定义值状态
    valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  }

  override def map(in: SensorReading): String = {
    //a.值状态的读取
    val myV = valueState.value()
    //a.值状态的更新
    valueState.update(in.temperature)

    //b.列表状态的获取
    val ints: lang.Iterable[Int] = listState.get()
    //b.列表状态的追加:可以追加单个值，或追加一个java的列表
    listState.add(1) //追加单个值
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list) //追加一个列表
    //b.列表状态的更新：直接删除之前的列表，更新为现在的列表
    listState.update(list)

    // c.map状态的操作
    val bool: Boolean = mapState.contains("sensor_1")
    val d: Double = mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)

    // d.聚合状态的获取
    reduceState.get()
    // d.调用ReduceFunction进行聚合
    reduceState.add(in)

    in.id
  }
}
/*

    public ValueStateDescriptor(String name, TypeSerializer<T> typeSerializer, T defaultValue) {
        super(name, typeSerializer, defaultValue);
    }

    public ValueStateDescriptor(String name, Class<T> typeClass) {
        super(name, typeClass, (Object)null);
    }
 */