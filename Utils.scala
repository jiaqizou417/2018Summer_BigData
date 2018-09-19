import scala.collection.mutable.ArrayBuffer

object Utils {
    // 识别报文请求类型（卡顿、缓存、开始播放、结束播放）
    val iqiyiPtn = Map(
        "stalling" -> "^/core\\?t=8&tm=(\\d+).*&aid=(\\d+).*".r,  //卡顿
        "cache" -> "^/videos/.*f4v\\?((.*key=.*?)|(.*src=iqiyi\\.com.*qd_index=1.*)){2}.*".r,  //缓存
        "startPlay" -> "^/core\\?t=1&.*&aid=(\\d+).*".r,
        "endPlay" -> "^/core\\?t=13&.*".r
    )

    // 根据卡顿特征码判断是否是卡顿报文
    def isStallingMsg(originArr: Array[String]): Boolean = {
        val uri_text = originArr(32)
        val stalling_pattern = ".*/core\\?t=8&tm=\\d+.*&rd=video.*".r // "tm=\\d+"为卡顿持续时间
        if (stalling_pattern.pattern.matcher(uri_text).matches()) {
            return true
        }
        false
    }

    // 从uri中提取视频id
    def getTvid(uri: String): String = {
        val pattern = ("(?:\\?|&)" + "aid" + "=(.*?)(?:&|$)").r
        for (pattern(value) <- pattern.findAllIn(uri)) {
            return value
        }
        "-1"
    }

    // 处理报文，返回元组
    def parseIqiyi(originArr: Array[String]): (String, String, String, String, String, String, String, String, String, String, String,
      String, String, String, String, String, String, String, String, String, String, String) = {
        val streamStartTime: String = originArr(0) // 话单事件开始时间，0
        val streamEndTime: String = originArr(1) // 话单事件结束时间，1
        val personID: String = originArr(2) // 运营商提供的用户账号，2

        //        val RAT: Int = originArr(5).toInt // 基站无线类型，5
        //        val terminal_type: Int = originArr(6).toInt // 终端无线类型，6
        //        val terminal_type2: Int = originArr(7).toInt // 终端分类，7
        //        val terminal_type3: Int = originArr(8).toInt // 终端品牌型号名（不包含厂商），8
        //        val terminal_brand: String = originArr(36) // 终端厂商，编解码有问题，36

        val LAC: String = originArr(14) // 基站信息：移动：10进制数字，电信：16进制数字字符串，14
        val CI: String = originArr(15) // 基站信息：移动：10进制数字，电信：16进制数字字符串，15

        //        val userIP: String = originArr(16) // 用户IP，有两种形式，IPv4和IPv6，16
        val serverIP: String = originArr(17) // 服务器IP，有两种形式，IPv4和IPv6，17
        val IP_protocol_type: String = originArr(18) // IP协议类型，1:ICMP,6:TCP,17:UDP，18
        val userPort: String = originArr(19) // 用户端口，19
        val serverPort: String = originArr(20) // 服务器端口，20

        val UL_msg_cnt: String = originArr(23) //上行报文数，23
        val DL_msg_cnt: String = originArr(24) // 下行报文数，24
        val UL_bytes: String = originArr(25) // 上行字节数，25
        val DL_bytes: String = originArr(26) // 下行字节数，26

        val UL_msg_cnt_rpt: String = originArr(27) //上行重传报文数，27
        val DL_msg_cnt_rpt: String = originArr(28) // 下行重传报文数，28
        val UL_bytes_rpt: String = originArr(29) // 上行重传字节数，29
        val DL_bytes_rpt: String = originArr(30) // 下行重传字节数，30

        val host: String = originArr(31) // 域名信息，31
        val uri: String = originArr(32) // 请求的uri，32
        val response_code: String = originArr(33) //请求应答码，33
        val response_log: String = originArr(34) // 请求应答时长，34
        val response_length: String = originArr(35) // 应答内容长度，35

        //        val tvid: String = getTvid(uri) // 视频ID
        //        val stallingLength: Int = -1L // 卡顿时长
        //        val resolution: String = "" // 视频分辨率
        //        val uid: String = "" // 用户id，未登录为空
        //        val operationType: String = "" // 用户操作类型
        //        val time_std: String = "" // 由TCP链接的开始时间转换得到的日期，格式：yyyy-MM-dd hh:mm

        //        val linkDuration: Int = streamEndTime - streamStartTime // 链接持续时间

        // msg为上报卡顿的一条报文
        val msg = (streamStartTime, streamEndTime, personID, LAC, CI, serverIP, IP_protocol_type, userPort, serverPort, UL_msg_cnt, DL_msg_cnt, UL_bytes,
          DL_bytes, UL_msg_cnt_rpt, DL_msg_cnt_rpt, UL_bytes_rpt, DL_bytes_rpt, host, uri, response_code, response_log, response_length)

        msg

    }

    // 处理话单，返回Data对象
    def parseData(msg: Array[String]): Data = {
        // one hot编码
        val isUDP: Int = if (msg(18) == "17") 1 else 0
        val isTCP: Int = if (msg(18) == "6") 1 else 0

        val beginTime = (msg(0).toDouble * 1000).toLong
        val endTime = (msg(1).toDouble * 1000).toLong
        val duration = endTime - beginTime
        val userId = msg(2)

        val uri = msg(32)
        val reqType = Utils.getReqType(msg(32))
        val aid = Utils.getAid(msg(32))
        val stallType = if (reqType == "stalling") 1 else 0
        val stallingTime = getStallingTime(uri)

        def toLong(x: String): Long = {
            x.split('.')(0).toLong
        }

        Data(userId, aid, beginTime, endTime, duration, isUDP, isTCP, toLong(msg(23)), toLong(msg(25)), toLong(msg(27)), toLong(msg(29)),
            toLong(msg(24)), toLong(msg(26)), toLong(msg(28)), toLong(msg(30)), reqType, stallType, stallingTime)
    }

    // 返回报文请求类型
    def getReqType(url: String): String = {
        for ((t, ptn) <- iqiyiPtn) {
            if (ptn.pattern.matcher(url).matches()) {
                return t
            }
        }
        "unknown"
    }

    // 获得视频编号
    def getAid(url: String): String = {
        val ptn = "^/core?.*aid=(\\d+).*".r
        for (ptn(aid) <- ptn.findAllIn(url)) {
            return aid
        }
        "-1"
    }

    // 获取卡顿（结束）时间
    def getStallingTime(url: String): Long = {
        val ptn = "^/core?.*tm=(\\d+).*".r
        for (ptn(time) <- ptn.findAllIn(url)) {
            return time.toLong
        }
        0L
    }

    def findReqForStalling(data: Array[Data], b: Int, e: Int, duration: (Long, Long)): Array[Data] = {
        var rs = ArrayBuffer[Data]()

        for (i <- b to e) {
            if (data(i).startTime > duration._1 && data(i).startTime < duration._2) {
                rs += data(i)
            }
        }
        rs.toArray
    }
}
