package com.atguigu.sparkmall0901.reeltime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsLog(ts:Long , area:String, city:String, userId:String, adsId:String) {
  //触发一次就可以
   val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //转成日期
  def getDate() ={
    dateFormat.format(new Date(ts))
  }
}
