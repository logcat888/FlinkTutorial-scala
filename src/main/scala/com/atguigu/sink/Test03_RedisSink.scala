package com.atguigu.sink


import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author chenhuiup
 * @create 2020-11-15 11:39
 */
/*
RedisSink：
1.Redis构造器参数解读
  1）flinkJedisConfigBase为Jedis连接配置
  2）RedisMapper<IN>定义向redis写入的数据是什么，以及写入数据的命令是什么
  public class RedisSink<IN> extends RichSinkFunction<IN>
 public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper)
 */
object Test03_RedisSink {
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

    //定义一个FlinkJedisConfigBase
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6397)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))

    // 4.任务执行
    env.execute("Test03_RedisSink")
  }
}

// 定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
  //定义保存数据写入redis的命令，使用Hash结构 HSET  表名（外层key）  内层KV
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }
  //将温度值指定为value
  override def getKeyFromData(data: SensorReading): String = data.temperature.toString

  //将id指定为key
  override def getValueFromData(data: SensorReading): String = data.id
}
