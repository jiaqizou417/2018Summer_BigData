import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object ExtractEachUserMsgs {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("1day_stallingUsersMsgs").setMaster("yarn-client")
        val sc = new SparkContext(conf)
        val inputFile_hlj_v3 = "/import_data/userflow/gn/cm/heilongjiang_v3/20160421/*"
        val UserSet: mutable.Set[String] = mutable.Set[String]()

        // 返回一个文档内容（行）的迭代器
        val phoneList = Source.fromFile("1day_phone_list").getLines()
        // 生成用户id的HashSet
        for (phoneNumber <- phoneList) {
            UserSet += phoneNumber
        }

        // 将查询表进行广播
        val broadList = sc.broadcast(UserSet)

        val userid_Data: RDD[(String, Data)] = sc.textFile(inputFile_hlj_v3)
          .map(_.split("\\t"))
          .filter(_.length == 37)
          .filter(x => broadList.value.contains(x(2))) // 此条报文属于卡顿用户的报文
          .map((msg) => {
            val userID = msg(2)
            val startTime = msg(0).toDouble
            val endTime = msg(1).toDouble
            val duration = endTime - startTime
            val isUDP: Int = if (msg(18) == "17") 1 else 0
            val isTCP: Int = if (msg(18) == "6") 1 else 0
            val ULCnt = msg(23).toLong //上行报文数，23
            val DLCnt = msg(24).toLong // 下行报文数，24
            val ULByte = msg(25).toDouble // 上行字节数，25
            val DLByte = msg(26).toDouble // 下行字节数，26

            val ULReCnt = msg(27).toLong //上行重传报文数，27
            val DLReCnt = msg(28).toLong // 下行重传报文数，28
            val ULReByte = msg(29).toDouble // 上行重传字节数，29
            val DLReByte = msg(30).toDouble // 下行重传字节数，30

            val uri = msg(32)

            val stalling_pattern = "^/core\\?t=8.* &tm=\\d+.*&rd=video.*".r // 卡顿结束，1
            val click_play_pattern = "^/core\\?t=15&reset=0.*".r // 点击播放，2
            val get_args = "^vps\\?tvid=\\d+.*".r // 获取参数，3
            val ads_start = ".*&ct=adstart.*".r // 广告开始，4
            val ads_end = ".*&ct=adend.*".r // 广告结束，5
            val cache = "^/videos/.*f4v\\?key=.*&src=iqiyi\\.com.*qd_index=1.*".r // 缓存，6
            val start_play = "^/core\\?t=1&reset=0.*".r // 开始播放，7
            val end_play = "^/core\\?t=13&reset=0.*".r // 结束播放，8

            var iqiyi_type = 0 // 若此条报文满足iqiyi的特征码，则根据不同特征将iqiyi_type置为不同值
            if (stalling_pattern.pattern.matcher(uri).matches()) {
                iqiyi_type = 1
            } else if (click_play_pattern.pattern.matcher(uri).matches()) {
                    iqiyi_type = 2
            } else if (get_args.pattern.matcher(uri).matches()) {
                iqiyi_type = 3
            } else if (ads_start.pattern.matcher(uri).matches()) {
                iqiyi_type = 4
            } else if (ads_end.pattern.matcher(uri).matches()) {
                iqiyi_type = 5
            } else if (cache.pattern.matcher(uri).matches()) {
                iqiyi_type = 6
            } else if (start_play.pattern.matcher(uri).matches()) {
                iqiyi_type = 7
            } else if (end_play.pattern.matcher(uri).matches()) {
                iqiyi_type = 8
            }

            (userID, Data(startTime, endTime, duration, isUDP, isTCP, ULCnt, ULByte, ULReCnt, ULReByte, DLCnt, DLByte, DLReCnt, DLReByte, iqiyi_type))

        }).persist()

        val a = userid_Data.groupByKey().mapValues(iter => {
            val list: Array[Data] = iter.toArray.sortBy(_.startTime)
            var result = ""
            for (i <- list.indices) {
                result = result + "|" + list(i).toString
            }
            result
        })

        a.repartition(1).saveAsTextFile(arg(1))

    }

    case class Data(startTime: Double, endTime: Double, duration: Double, isUDP: Int, isTCP: Int, ULCnt: Long, ULByte: Double, ULReCnt: Long, ULReByte: Double,
                    DLCnt: Long, DLByte: Double, DLReCnt: Long, DLReByte: Double, iqiyi_type: Int) extends Serializable {
        override def toString: String = {
            Array(startTime, endTime, duration, isUDP, isTCP, ULCnt, ULByte, ULReCnt, ULReByte, DLCnt, DLByte, DLReCnt, DLReByte, iqiyi_type).mkString(",")
        }
    }

}