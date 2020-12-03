package com.atguigu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-09 23:16
 */
/*
reduce算子（实现更加复杂的归约需求）：需要输出当前最小的温度值，以及最近的时间戳，要用reduce
1. 有两个重载的方法，一个是匿名函数，另一个是函数类，需要实现ReduceFunction<T>接口，泛型为数据类型
public interface ReduceFunction<T> extends Function, Serializable {
    T reduce(T var1, T var2) throws Exception;
}
2. DataStream没有min、max、sum、reduce等方法，经过keyBy转换为KeyedStream后，就拥有了聚合方法。
3. KeyedStream内部有一个私有的方法aggregate，其他聚合方法（min、max、sum、reduce...）底层都是调用aggregate方法。
4. 所有的聚合方法，都是累计状态与新数据进行运算得到新的状态。
 */
object Test05_Reduce {
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
    // 需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id") //根据id进行分组，也可以传入索引
      // a.使用Lambda表达式实现reduce
//      .reduce((curState,newData) =>{
//        SensorReading(curState.id,newData.timestamp,Math.min(curState.temperature,newData.temperature))
//      })
      // b.使用函数类实现reduce
        .reduce(new MyReduceFunction)

    // 4. 打印
    reduceStream.print()

    // 5.执行任务
    // jobName也可以不传
    env.execute("aaa")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading =
    SensorReading(t.id,t1.timestamp,t.temperature.min(t1.temperature))
}


/*
  def reduce(reducer: ReduceFunction[T]): DataStream[T] = {
    if (reducer == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }

    asScalaStream(javaStream.reduce(reducer))
  }

   * Creates a new [[DataStream]] by reducing the elements of this DataStream
   * using an associative reduce function. An independent aggregate is kept per key.
  def reduce(fun: (T, T) => T): DataStream[T] = {
    if (fun == null) {
      throw new NullPointerException("Reduce function must not be null.")
    }
    val cleanFun = clean(fun)
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T) : T = { cleanFun(v1, v2) }
    }
    reduce(reducer)
  }

 */