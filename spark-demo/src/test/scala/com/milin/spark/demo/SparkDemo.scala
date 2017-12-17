package com.milin.spark.demo

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.util.Random

/**
  * Created by milin on 2017-08-27.
  */
class SparkDemo {

  @Test
  def constructTestData(): Unit = {
    val dataFile = new File("/tmp/sparkDemoFile")
    if (dataFile.exists()) {
      dataFile.delete()
    }
    val writer = new BufferedWriter(new FileWriter(dataFile))
    (1 to 100000).foreach(i => {
      writer.write(s"13${Random.nextInt(900000000) + 100000000}")
      writer.newLine()
      writer.flush()
    })
    writer.close()
  }

  /**
    * 统计每个手机号段出现的频次
    */
  @Test
  def sparkTest1():Unit = {
    val conf:SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("spark-test1") //以本地计算机为spark计算节点
    val sc:SparkContext = new SparkContext(conf)
    sc.textFile("/tmp/sparkDemoFile") //加载待计算的本地文件
//        .repartition(10) //设置可并行分片数，由于数据量较小，且只有本地一个节点，所以不用
        .distinct() //去重
        .map(_.substring(0, 3)) // 截取前四位
        .map((_, 1)) // 每个值计数1
        .reduceByKey(_ + _) //相同键的计数相加
        .toLocalIterator.toList.sorted //转换为本地List并排序
        .foreach(println) //打印到本地
  }

  /**
    * 通过spark sql的方式，查询138号段，包含666的100个手机号码
    */
  @Test
  def sparkTest2():Unit = {
    val conf:SparkConf = new SparkConf()
      .setMaster("yarn-client")
      .setAppName("spark-test2") //以本地计算机为spark计算节点
    val sc:SparkContext = new SparkContext(conf)
    val sqlContext:SQLContext = new SQLContext(sc)
    import sqlContext.implicits._ // import spark sql需要用到的隐式转换
    val dataRdd = sc.textFile("/tmp/sparkDemoFile") //加载待计算的本地文件
    dataRdd.map(s => (s, s.substring(0, 3))) //映射第一个字段为手机号，第二个字段为手机前三位
      .toDF().registerTempTable("tab1") //注册为临时表 tab1
    sqlContext.sql("select `_1`, `_2` from `tab1` where `_1` like '%666%' and `_2` = '138'") //执行sql查询
      .rdd // 结果转为rdd
      .map(_.getString(0)) // 从结果中取第一个值
      .take(100) //取结果数据中的一百个到本地
      .sorted
      .foreach(println) //排序并打印
  }

}
