package demo

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 监控指定文件夹，单词计数
  */
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
//    val conf=new SparkConf().setMaster("spark://192.168.154.10:7077").setAppName("SparkStreamingDemo");
    val conf=new SparkConf().setAppName("SparkStreamingDemo");
    val sc=new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.textFileStream("file:///home/pros/stream")
    val words = lines.flatMap(_.split(" "));
    val wordCounts = words.map((_,1)).reduceByKey(_+_);
    wordCounts.print();
    ssc.start();

    ssc.awaitTermination()  //保持SparkStreaming一直开启，直到用户手动中断
  }
}
