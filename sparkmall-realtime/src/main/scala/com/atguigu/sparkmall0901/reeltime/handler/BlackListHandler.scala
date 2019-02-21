package com.atguigu.sparkmall0901.reeltime.handler

import java.util
import java.util.Properties

import com.atguigu.sparkmall0901.common.utils.PropertiesUtil
import com.atguigu.sparkmall0901.reeltime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  def handle(absLongDsream: DStream[AdsLog]) = {

    //每日每用户的点击次数      3个组成的key   val为1L   DSream
    val clickcountPerAdsPerDayDStream: DStream[(String, Long)] = absLongDsream.map { adsLong => (adsLong.getDate() + "_" + adsLong.userId + "_" + adsLong.adsId, 1L) }.reduceByKey(_ + _)

    clickcountPerAdsPerDayDStream.foreachRDD( rdd => {

      val prop: Properties = PropertiesUtil.load("config.properties")

      rdd.foreachPartition { adsItr =>
        //建立redis连接
        val jedis = new Jedis(prop.getProperty("redis.host"), prop.getProperty("redis.port").toInt) //driver端运行
        //redis结构   hash  key  field    value
        adsItr.foreach { case (logkey, count) =>
          val day_user_ads: Array[String] = logkey.split("_")
          val day = day_user_ads(0)
          val user = day_user_ads(1)
          val ads = day_user_ads(2)
          val key = "user_ads_click:" + day

          //自定义步长自增   否则数据会被覆盖
          jedis.hincrBy(key, user + "_" + ads, count)

          //判断  是否达到了 100
          val curCount: String = jedis.hget(key, user + "_" + ads)

          if (curCount.toLong >= 100) {
            //黑名单key的类型  :set   无序   不可重复
            jedis.sadd("blacklist", user)
          }
        }
        jedis.close()
      }

    }
    )

  }


  def check(sparkContext: SparkContext, adsLogDstream: DStream[AdsLog]): DStream[AdsLog] ={

    //利用filter过滤   过滤依据是 blacklist

    ///      此部分的driver代码只会在启动时执行一次，会造成blacklist无法实时更新
    //    val prop: Properties = PropertiesUtil.load("config.properties")
    //    val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
    //    val blacklistSet: util.Set[String] = jedis.smembers("blacklist")  //driver
    //    val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
    //    val filteredAdsLogDstream: DStream[AdsLog] = adsLogDstream.filter { adslog =>
    //      !blacklistBC.value.contains(adslog)   //executor
    //    }


    val filteredAdsLogDstream: DStream[AdsLog] = adsLogDstream.transform{rdd=>
      // driver 每个时间间隔执行一次
      val prop: Properties = PropertiesUtil.load("config.properties")
      val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
      val blacklistSet: util.Set[String] = jedis.smembers("blacklist")  //driver
    val blacklistBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blacklistSet)
      rdd.filter{ adslog =>
        println(blacklistBC.value)
        !blacklistBC.value.contains(adslog.userId)   //executor
      }


    }


    filteredAdsLogDstream
  }



}
