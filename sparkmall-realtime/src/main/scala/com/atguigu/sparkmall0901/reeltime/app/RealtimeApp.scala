package com.atguigu.sparkmall0901.reeltime.app

import com.atguigu.sparkmall0901.common.utils.MyKafkaUtil
import com.atguigu.sparkmall0901.reeltime.bean.AdsLog
import com.atguigu.sparkmall0901.reeltime.handler.{AreaCityAdsCountHandler, AreaTop3AdsHandler, BlackListHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //实时
    val ssc = new StreamingContext(sc, Seconds(5))

    sc.setCheckpointDir("./checkpoint")

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)
    /*    //测试
        recordDstream.map(_.value()).foreachRDD{ rdd =>
          println(rdd.collect().mkString("\n"))
        }*/
    val adsLogDstream: DStream[AdsLog] = recordDstream.map(_.value()).map { log =>

      val logArr: Array[String] = log.split(" ")
      AdsLog(logArr(0).toLong, logArr(1), logArr(2), logArr(3), logArr(4))
    }

    //先过滤
    val filteredAdsLogDstream: DStream[AdsLog] = BlackListHandler.check(sc, adsLogDstream)
    //做统计
    BlackListHandler.handle(filteredAdsLogDstream)
    //处理业务  需求五    这里计算到了城市级别
    val areaCityAdsCountDstream: DStream[(String, Long)] = AreaCityAdsCountHandler.handler(filteredAdsLogDstream)

    //处理业务   需求六  每天各地区 top3 热门广告
    AreaTop3AdsHandler.handle(areaCityAdsCountDstream)

    ssc.start()
    ssc.awaitTermination()

  }
}
