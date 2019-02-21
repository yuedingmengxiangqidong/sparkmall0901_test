package com.atguigu.sparkmall0901.offline.app


import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall0901.common.bean.UserVisitAction
import com.atguigu.sparkmall0901.common.utils.{JdbcUtil, PropertiesUtil}
import com.atguigu.sparkmall0901.offline.acc.CategoryAccumulator
import com.atguigu.sparkmall0901.offline.bean.CategoryCount
import com.atguigu.sparkmall0901.offline.handler.{CategoryCountHandler, CategoryTopSessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object OffineApp {

  def main(args: Array[String]): Unit = {
    //多个报表的情况下
    val taskId: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("sparkmall-offline").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //1 根据条件把hive中数据查询出来
    // 得RDD[UserVisitAction
    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession)
/*    //测试使用
        userVisitAction.foreach { userVisitAction =>
          println(userVisitAction)
        }*/
    //需求一
    val categoryCountList: List[CategoryCount] = CategoryCountHandler.handle(sparkSession,userVisitActionRDD, taskId)
    println("需求一完成!!")

    //需求二
    CategoryTopSessionHandler.handle(sparkSession,userVisitActionRDD,taskId,categoryCountList)
    println("需求二完成")


  }

  def readUserVisitActionToRDD(sparkSession: SparkSession): RDD[UserVisitAction] = {
    //json    解析
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val conditions: String = properties.getProperty("condition.params.json")

    val conditionJsonObj: JSONObject = JSON.parseObject(conditions) //解析

    val startDate: String = conditionJsonObj.getString("startDate")
    val endDate: String = conditionJsonObj.getString("endDate")
    val startAge: String = conditionJsonObj.getString("startAge")
    val endAge: String = conditionJsonObj.getString("endAge")

    //where 1 =1 没有效果,只是为了语法不出错
    var sql = new StringBuilder("select v.* from user_visit_action v,user_info u where v.user_id = u.user_id")

    if (startDate.nonEmpty) {
      sql.append(" and date >= '" + startDate + "'")
    }

    if (endDate.nonEmpty) {
      sql.append(" and date <= '" + endDate + "'")
    }

    if (startAge.nonEmpty) {
      sql.append(" and age >= " + startAge) //数字不需要加单引号
    }

    if (endAge.nonEmpty) {
      sql.append(" and age <= " + endAge)
    }
    println(sql)

    sparkSession.sql("use sparkmall0901")
    import sparkSession.implicits._
    //通过as完成的转化
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

    rdd
  }

}
