package BNLV_learning.Global

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Gao on 2016/8/27.
  */
object SparkConf {
  /** Spark环境配置 */
  val conf = new SparkConf().setAppName("BNLV Learning Based on Spark")

  conf.setMaster("local[16]")

  // TaskSetManager: Total size of serialized results of 89 tasks (1027.1 MB) is bigger than spark.driver.maxResultSize (1024.0 MB)
  //  Cause:
  //  caused by actions like RDD’s collect() that send big chunk of data to the driver（不一定是因为RDD的问题哦~）
  //  Solution:
  //  set by SparkConf: conf.set("spark.driver.maxResultSize", "3g")
  //  set by spark-defaults.conf: spark.driver.maxResultSize 3g
  //  set when calling spark-submit: --conf spark.driver.maxResultSize=3g
  conf.set("spark.driver.memory", "8g")
  conf.set("spark.executor.memory", "8g")
  conf.set("spark.driver.maxResultSize", "8g")
  conf.set("spark.dynamicAllocation.minExecutors","1")  //最少的executor个数
  conf.set("spark.dynamicAllocation.maxExecutors","1")  //最大的executor个数  根据自己实际情况调整
  conf.set("spark.dynamicAllocation.initialExecutors","1")//初始executor个数
  val sc = new SparkContext(conf)
  // -Xms8G -Xmx16G -XX:PermSize=512M -XX:MaxPermSize=16G
//
//    val sc = new SparkContext(new SparkConf())

  sc.setLogLevel("ERROR")
  var partitionNum = 128                       //数据分区数目
}
