package com.atguigu.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @author chenhuiup
 * @create 2020-11-15 14:48
 */
/*
自定义Sink实现MySQL的连接:
尽管通过自定义Sink实现Mysql的数据插入很简单但是Flink没有提供官方的依赖，是因为写入数据简单但是分布式的容错性和正确性很保证，
所以Flink没有实现MySQL的连接。
 */
object Test05_JdbcSink {
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

   dataStream.addSink(new MyJdbcSinkFunc())

    // 4.任务执行
    env.execute("Test04_EsSink")
  }

}
/*
自定义Sink：
1. 继承RichSinkFunction<IN>的原因是富函数有运行时上下文，另外还有生命周期方法，与外部数据库建立连接
  不能每来一条数据就建立一次，因此在生命周期open/close中建立连接，关闭连接。
*/
class MyJdbcSinkFunc extends RichSinkFunction[SensorReading]{

//定义连接、预编译语句
  var conn:Connection = _
  var insertStmt:PreparedStatement =_
  var updateStmt:PreparedStatement =_

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id,temp) values(?,?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp=? where id=?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
      //先执行更新操作，查到就更新
    updateStmt.setDouble(1,value.timestamp)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    //如果更新没有查到数据，那么就插入
    if (updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.timestamp)
      insertStmt.execute()
    }
  }

  // 归还连接
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
