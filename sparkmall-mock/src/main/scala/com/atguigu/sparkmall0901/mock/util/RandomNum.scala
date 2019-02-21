package com.atguigu.sparkmall0901.mock.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复

    if (canRepeat) {

      val numList = new ListBuffer[Int]() //链表

      while (numList.size < amount) {
        numList += apply(fromNum: Int, toNum: Int)
      }
      numList.mkString(delimiter)
    }else {
      val numSet = new mutable.HashSet[Int]() //集合
      while(numSet.size < amount) {
        numSet += apply(fromNum, toNum)
      }

      numSet.mkString(delimiter)

    }

  }

  def main(args: Array[String]) ={

    //false为不可以重复,true为可以重复
    print(multi(1,5,3, ",", false))
  }

}
