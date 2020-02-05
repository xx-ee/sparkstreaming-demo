package kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * SparkStreaming与Kafka整合
  */
object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    //--如果用spark本地单机模式去消费kafka数据
    //--注意:local[N] N表示启动的线程数，至少是2个。
    //--其中一个线程负责运行SparkStreaming,另外一个线程负责消费Kafka数据
    //--如果只有一个线程，则消费不到Kafka数据
    val conf=new SparkConf().setMaster("local[2]").setAppName("streamkafka")
    val sc=new SparkContext(conf)

    val ssc=new StreamingContext(sc,Seconds(10))

    //--指定zk集群列表
    val zkHosts="192.168.2.10:2181,192.168.2.11:2181,192.168.2.12:2181"
    //--指定消费者组名
    val group="gp1"

    //--指定消费的主题，key是主题名，value是消费的线程数
    val topics=Map("music"->1,"enbook"->1)


    //--通过工具类去消费Kafka数据
    val data=KafkaUtils.createStream(ssc, zkHosts, group, topics)
      .map{x=>x._2}


    data.print

    ssc.start

    ssc.awaitTermination()

  }
}
