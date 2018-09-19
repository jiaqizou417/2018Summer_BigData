import Test.{getClassifyResult, getValidTime}
import org.apache.spark.{SparkConf, SparkContext}

object firstfilter_iqiyi {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("iqiyi-filter").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val ptn1 = ".*.qpic.cn.*".r
    //host
    val ptn2 = ".*.iqiyi.com.*".r
    //host
    val ptn3 = "^msg.71.am$".r
    //host
    val ptn4 = "^/videos/.*".r
    //uri

    def iqiyiFilter(line: String): Boolean = {
      val lineSplited = line.split("\\t")

      if (is(ptn1, lineSplited(22)) || is(ptn2, lineSplited(22)) || is(ptn3, lineSplited(22))) {
        return true
      }
      else if (is(ptn4, lineSplited(23))) return true
      else
        return false
    }


    def is(ptn: Regex, s: String): Boolean = ptn.pattern.matcher(s).matches()

    sc.textFile("E:\\spark\\spark_project\\firstfilter_iqiyi_part-00000")
      .filter(x => x.split("\\t").length == 41
        && !x.split("\\t")(14).equals("-") && !x.split("\\t")(15).equals("-") && !x.split("\t")(30).equals("-")
        && !x.split("\\t")(31).equals("-") && !x.split("\\t")(32).equals("-") && !x.split("\\t")(33).equals("-"))
      .filter(iqiyiFilter).repartition(1).saveAsTextFile("iqiyi-filter")
  }
  }

