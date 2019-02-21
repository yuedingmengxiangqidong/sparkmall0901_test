package com.atguigu.sparkmall0901.reeltime.handler



import java.util.Properties

import com.atguigu.sparkmall0901.common.utils.PropertiesUtil
import com.atguigu.sparkmall0901.reeltime.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object AreaCityAdsCountHandler {



  def handler(absLongDsream: DStream[AdsLog]):DStream[(String, Long)] = {

    //1. 整理 dstream = kv 结构 (date:area:ads,1L)
    val adsClickDstream: DStream[(String, Long)] = absLongDsream.map { adslog =>
      val key = adslog.getDate() + ":" + adslog.area + ":" + adslog.city + ":" + adslog.adsId
      (key, 1L)
    }
    //2.利用updateStateBykey进行累加
    val adsClickCount: DStream[(String, Long)] = adsClickDstream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
      // 把countSeq汇总, 累加在tatal里

      println(countSeq.mkString(","))

      val countSum: Long = countSeq.sum
      val curTotal = total.getOrElse(0L) + countSum
      Some(curTotal)
    }
    // 3.把结果保存到redis 中
    adsClickCount.foreachRDD { rdd =>
      val prop: Properties = PropertiesUtil.load("config.properties")
      rdd.foreachPartition { adsClickCountItr =>
        //建立redis连接
        val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
        adsClickCountItr.foreach { case (key, count) =>
          jedis.hset("date:area:city:ads", key, count.toString)
        }
        jedis.close()
      }
    }
      //返回值   让需求六使用
    adsClickCount

  }

}
