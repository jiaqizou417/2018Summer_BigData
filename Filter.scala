import org.apache.spark.{SparkConf, SparkContext}

object Filter {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("yjx")
        val sc = new SparkContext(conf)
        val inputFile_hlj_v3 = args(0)
        val ptn = ".*core\\?.*".r

        sc.textFile(inputFile_hlj_v3)
          .map(_.split("\\t"))
          .filter(_.length == 37)
          .filter(_ (2) != "10.0.0.172") // 此ip含有大量访问，感觉是异常
          .filter((x) => ptn.pattern.matcher(x(32)).matches())
          .map(_ (32))
          .repartition(1)
          .saveAsTextFile(args(1))
    }

}
