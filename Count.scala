import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Count {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("zhangkai")
        val sc = new SparkContext(conf)
        val inputFile_hlj_v3 = <find a place to read data>
        // val UserSet: mutable.Set[String] = mutable.Set[String]()
        // 返回一个文档内容（行）的迭代器
        // val phoneList = Source.fromFile("phone_list").getLines()
        // 生成用户id的HashSet
        // for (phoneNumber <- phoneList) {
        //     UserSet += phoneNumber
        // }

        // 将查询表进行广播
        val broadList = sc.broadcast(UserSet)

        sc.textFile(inputFile_hlj_v3)
          .map(_.split("\\t"))
          .filter(_.length == 37)
          .filter(x => broadList.value.contains(x(2)))
          .filter(_ (2) != "10.0.0.172") // 此ip含有大量访问，感觉是异常
          .map(Utils.parseData)
          .filter((x) => x.reqType == "startPlay" || x.reqType == "stalling")
          .groupBy(_.userId)
          .flatMap((x) => {
              var rs = ArrayBuffer[(String, Long, Long, Long, Long)]()
              val data = x._2.toArray.sortBy(_.startTime)
              for (i <- data.indices) {
                  if (data(i).reqType == "stalling") {
                      var k = i

                      while (k >= 0
                        && data(k).reqType != "startPlay"
                        && data(k).aid == data(i).aid)
                          k -= 1

                      if (k >= 0 && data(k).reqType == "startPlay"
                        && data(k).aid == data(i).aid) {
                          rs.append((data(i).aid, data(k).startTime, data(i).startTime, data(i).startTime - data(k).startTime, data(i).stallingTime))
                      }
                  }
              }
              rs
          })
          .map(_.toString())
          .repartition(1)
          .saveAsTextFile("<find a place to save>")

    }

}


