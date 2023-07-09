package com.fabulouslab.spark.utils

import org.scalatest._

class DateUtilsSpec extends FlatSpec with Matchers {

  "A" should "pop values in last-in-first-out order" in {
    val unit = DateUtils.toDate("19.09")
    println(unit)
    2 shouldBe  (2)
  }


}
