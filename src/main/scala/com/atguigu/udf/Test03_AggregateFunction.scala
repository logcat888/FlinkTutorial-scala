package com.atguigu.udf

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @author chenhuiup
 * @create 2020-11-21 23:50
 */
/*
自定义一个聚合函数，求每个传感器的平均温度值
1. 注意导包import org.apache.flink.table.functions.AggregateFunction
2. 聚合函数输出时，必须使用toRetractStream，因为需要更新状态而不是追加。
3. aggregate的形参列表可以传入Symbol表达式
4. 自定义聚合函数的过程：
  1）定义一个类继承AggregateFunction<T, ACC>泛型解读：T代表输出数据类型，ACC每次聚合后的状态值；
  2）重写两个方法getValue和createAccumulator；
  3）自定义一个方法def accumulate(accumulator:AvgTempAcc,temp:Double):Unit，形参列表分别为聚合状态，输入数据类型。
  4）定义一个类，专门用于表示聚合的状态，因为accumulate方法的形成列表是不可变型的，如果是元组类型或其他值类型就无法更新状态
 */
object Test03_AggregateFunction {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() //在1.10默认使用old planner，在1.11默认使用BlinkPlanner
      .inStreamingMode() //使用流处理环境
      .build()

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env,settings)

    // 2.读取数据
    val inputDataStream = env.readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3.转换数据结构
    val dataStream: DataStream[SensorReading] = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }) //提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    // 4. 从流中创建表时指定时间特性,可以调换字段顺序，并追加时间戳字段成为伪列
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts )

    // 5.table api
    // a.创建AggregateFunction对象
    val avgTemp = new AvgTemp()
    // b.
    val resultTable = sensorTable
      .groupBy('id) // 分组，统计每个传感器的温度平均值
      .aggregate(avgTemp('temperature) as 'avgTemp) // 使用聚合函数，并将聚合值起别名，'avgTemp为symbol表达式
      .select('id, 'avgTemp)

    // 6.sql
    // a.注册表
    tableEnv.createTemporaryView("sensor",sensorTable)
    // b.注册函数
    tableEnv.registerFunction("avgTemp",avgTemp)
    // c.执行sql
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        | id, avgTemp(temperature)
        |from sensor
        |group by id
        |""".stripMargin)

    // 7.打印
    resultTable.toRetractStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sql")
    /*
sql> (true,sensor_1,35.8)
result> (true,sensor_1,35.8)
sql> (true,sensor_6,15.4)
result> (true,sensor_6,15.4)
sql> (true,sensor_7,6.7)
sql> (true,sensor_10,38.1)
result> (true,sensor_7,6.7)
sql> (false,sensor_1,35.8)
result> (true,sensor_10,38.1)
sql> (true,sensor_1,33.9)
sql> (false,sensor_1,33.9)
result> (false,sensor_1,35.8)
sql> (true,sensor_1,34.666666666666664)
     */

    // 8.执行任务
    env.execute()
  }

}

// 定义一个类，专门用于表示聚合的状态，因为accumulate方法的形成列表是不可变型的，如果是元组类型就无法更新状态
class AvgTempAcc{
  var sum:Double=_
  var count:Int = _
}

// 自定义一个聚合函数，求每个传感器的平均温度值，保存状态(tempSum,tempCount)，以便增加聚合时求平均值
/*
AggregateFunction<T, ACC>泛型解读：T代表输出数据类型，ACC每次聚合后的状态值
 */
class AvgTemp extends AggregateFunction[Double,AvgTempAcc]{
  // 获取输出值
  override def getValue(acc: AvgTempAcc): Double = acc.sum / acc.count

  // 初始化聚合状态
  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  // 还要实现一个具体的处理计算函数，函数名必须叫 accumulate,返回值类型为Unit
  def accumulate(accumulator:AvgTempAcc,temp:Double):Unit={
    accumulator.sum += temp;
    accumulator.count += 1;

  }
}

/*
	 * {@code
	 *   val aggFunc = new MyAggregateFunction
	 *   table.groupBy('key)
	 *     .aggregate(aggFunc('a, 'b) as ('f0, 'f1, 'f2))
	 *     .select('key, 'f0, 'f1)
	 * }
	 * </pre>
AggregatedTable aggregate(Expression aggregateFunction);
 */