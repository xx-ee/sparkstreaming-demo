package demo

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * checkpoint历史数据累加计算wordcount
  */
object SparkStreamingDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkStreamingDemo2");
    val sc =new SparkContext(conf);
    val ssc=new StreamingContext(sc,Seconds(5))
    //为了实现对历史批次数据的累加，需要指定一个目录来存储历史数据
    //该路径指定为本地路径，也可以指定在HDFS
    ssc.checkpoint("file:///home/pros/streamData")
    val data = ssc.textFileStream("file:///home/pros/stream")
    val r1=data.flatMap { _.split(" ") }.map { (_,1)}
    val r2=r1.updateStateByKey{(seq,op:Option[Int])=>
      Some(seq.sum+op.getOrElse(0))}

    r2.print

    ssc.start()

    ssc.awaitTermination()
  }
}
