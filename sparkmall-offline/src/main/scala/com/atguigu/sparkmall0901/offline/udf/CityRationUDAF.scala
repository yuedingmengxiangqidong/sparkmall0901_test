package com.atguigu.sparkmall0901.offline.udf


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CityRationUDAF extends UserDefinedAggregateFunction{
  //自定义输入 类型String
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))
  //定义存储类型 类型 Map, Long
  override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType) ))
  //定义输出类型 String
  override def dataType: DataType = StringType
  //验证  是否相同的输入有相同的输出,如果一样就返回true
  override def deterministic: Boolean = true
  //存储的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap[String,Long]
    buffer(1) = 0L

  }
  //跟新  每到一条数据做一次跟新   输入 加入存储     在Executor中执行
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val cityName = input.getString(0)

    buffer(0) = cityCountMap + (cityName -> (cityCountMap.getOrElse(cityName, 0L) + 1L))
    buffer(1) = totalCount + 1L

  }

  //driver 中的合并   合并 每个分区处理完成  汇总到driver时进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)
    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    buffer1(0) = cityCountMap1.foldLeft(cityCountMap2) {case (cityCountMap2, (cityName1,count1)) =>
      cityCountMap2 + (cityName1 -> (cityCountMap2.getOrElse(cityName1, 0L) + count1))
    }

    buffer1(1) = totalCount1 + totalCount2
  }

  //把存储的数据展示出来     方法本身内部使用,和UDAF没啥关系  map这时候可以使用可变的
  override def evaluate(buffer: Row): Any = {
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    // 1.计算百分比
    val cityRationInfoList: List[CityRationInfo] = cityCountMap.map { case (cityName, count) =>
      val cityRation = math.round(count.toDouble / totalCount * 1000) / 10D
      CityRationInfo(cityName, cityRation)
    }.toList

    //2.排序截取   前二
    var cityRationInfoTop2List: List[CityRationInfo] = cityRationInfoList.sortWith { (cityRationInfo1, cityRationInfo2) =>
      cityRationInfo1.cityRatio > cityRationInfo2.cityRatio
    }.take(2)


    //3.把其他计算出来
    if (cityRationInfoList.size > 2) {
      var otherRatio = 100D
      cityRationInfoTop2List.foreach(CityRationInfo => otherRatio -= CityRationInfo.cityRatio)
      //四舍五入
      otherRatio=math.round(otherRatio * 10) / 10D
      cityRationInfoTop2List = cityRationInfoTop2List :+ CityRationInfo("其他", otherRatio)
    }

    //4.拼接字符串
    cityRationInfoTop2List.mkString(",")

  }

  case class CityRationInfo(cityName:String, cityRatio: Double) {
    override def toString: String = {
      cityName + ":" + cityRatio + "%"
    }
  }

}
