import org.apache.spark.{SparkConf, SparkContext}

object ExtractUsers {

    def main(args: Array[String]) {

        val conf: SparkConf = new SparkConf().setAppName("Read_HLJ_14days").setMaster("yarn-client")
        val sc = new SparkContext(conf)
        val inputFile_hlj_v3 = "/import_data/userflow/gn/cm/heilongjiang_v3/201604{1[6-9],2[0-9]}/*" // 选取2016年4月16-19日，20-29日的话单数据

        val stallingRDD = sc.textFile(inputFile_hlj_v3)
          .map(_.split("\\t"))
          .filter(_.length == 37)
          .filter(Utils.isStallingMsg)
          .map(x => x(2))
          .filter(_.startsWith("86")).distinct()
          .persist()

        stallingRDD.repartition(1).saveAsTextFile("<find a place to save>")
    }

}