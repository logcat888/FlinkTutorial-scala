package com.atguigu.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @author chenhuiup
 * @create 2020-11-08 9:38
 */
/*
批处理的word count
1. 手动导入 import org.apache.flink.api.scala._ ，以便能够使用scala中定义的隐式转换
2. Flink有java版和scala版的API，且类名相同，因此导包过程中需要注意是引入的是scala包中的内容，还是java包中的内容
3. groupBy、sum都是根据字段进行运算。
4. 批处理使用的是DataSetAPI，流处理使用的是DataStreamAPI
5. 批处理的执行环境读取文本文件
 */
object Test01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个批处理的执行环境，因为是分布式架构处理流式数据，所以需要执行环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2.从文件中读取数据
    val inputPath = "D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 3.对数据进行转换处理统计，先分词，再按照word进行分组，最后进行聚合统计
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) //以第一个元素作为key，进行分组
      .sum(1)  //对所有数据的第二个元素求和

    // 4.打印输出
    resultDataSet.print()
  }
}

/*
  注：groupBy是按照字段索引作为key
  def groupBy(fields: Int*): GroupedDataSet[T] = {
    new GroupedDataSet[T](
      this,
      new Keys.ExpressionKeys[T](fields.toArray, javaSet.getType))
  }
 */