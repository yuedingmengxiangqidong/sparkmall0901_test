package com.atguigu.sparkmall0901.offline.handler

import com.atguigu.sparkmall0901.common.bean.UserVisitAction
import com.atguigu.sparkmall0901.common.utils.JdbcUtil
import com.atguigu.sparkmall0901.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTopSessionHandler {      //需求二

  def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String, top10CategoryList: List[CategoryCount]) = {

    val cidTop10: List[Long] = top10CategoryList.map(_.categoryId.toLong) //这个运行在Driver
    //广播变量
    val cidTop10BC: Broadcast[List[Long]] = sparkSession.sparkContext.broadcast(cidTop10)

    // 1. RDD[UserVisitAction] 过滤, 保留top10品类的点击
    val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction => //这个运行在Executor中
      //注意集合中的元素类型和  被比较类型 要一致
      cidTop10BC.value.contains(userVisitAction.click_category_id)
    }
    filteredUserVisitActionRDD


    // 2 RDD[UserVisitAction]统计次数   得到每个session点击   top10品类的次数
    //RDD-k-v结构
    val clickCountGroupByCidSessionRDD: RDD[(String, Long)] = filteredUserVisitActionRDD.map(action => (action.click_category_id + "_" + action.session_id, 1L)).reduceByKey(_ + _)

    //3分组  准备做组内排序  以品类Id 分组
    //3 RDD[action category_click_id + "_" + action.sessionID.count] - map
    val sessionCountGroupbyCidRdd: RDD[(String, Iterable[(String, Long)])] = clickCountGroupByCidSessionRDD.map { case (cidSession, count) =>
      val cidSessionArr: Array[String] = cidSession.split("_")
      val cid = cidSessionArr(0)
      val sessionId: String = cidSessionArr(1)
      (cid, (sessionId, count))
    }.groupByKey()

    //4小组赛 保留每组的前十名       //flatMap   打碎集合
    val sessionTop10RDD: RDD[Array[Any]] = sessionCountGroupbyCidRdd.flatMap { case (cid, sessionItr) =>
      val sessionTop10List: List[(String, Long)] = sessionItr.toList.sortWith { (sessionCount1, sessionCount2) =>
        sessionCount1._2 > sessionCount2._2
      }.take(10)
      //调整结构按照最终要保存的结构,填充session信息
      val sessionTop10ListWithCidList = sessionTop10List.map { case (sessionId, clickcount) =>
        Array(taskId, cid, sessionId, clickcount)
      }
      sessionTop10ListWithCidList
    }

    val sessionTop10Arr: Array[Array[Any]] = sessionTop10RDD.collect()

    //保存到mysql
   JdbcUtil.executeBatchUpdate("insert into category_top10_session_top10 values(?,?,?,?)" , sessionTop10Arr)

  }
}
