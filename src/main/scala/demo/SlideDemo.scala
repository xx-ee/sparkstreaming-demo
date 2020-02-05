package demo

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 滑动窗口机制
  * 每隔一段时间(滑动区间)计算下一个段时间(窗口长度)的数据
  */
object SlideDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("SlideDemo")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    //--窗口机制也是需要对历史数据累加，所以也需要指定目录
    ssc.checkpoint("file:///home/pros/streamData")
    val data= ssc.textFileStream("file:///home/pros/stream")

    val r1=data.flatMap { _.split(" ") }.map { (_,1)}
    //--①参:匿名函数,指定key的value需要如何计算 ②参:滑动区间 ③参:窗口长度
    val r2=r1.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Seconds(10),Seconds(10))

    r2.print

    ssc.start

    ssc.awaitTermination()
  }
}
