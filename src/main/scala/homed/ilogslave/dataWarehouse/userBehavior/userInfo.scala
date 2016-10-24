package homed.ilogslave.dataWarehouse.userBehavior

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.JdbcRDD
import homed.config.ReConfig._
import java.text.SimpleDateFormat
import java.util.Calendar
import homed.Options
import java.lang.Math
import homed.Jdbc.showJdbc
import homed.tools._
import homed.Jdbc.userJdbc

//用户信息
case class account_info(
    DA:Long,
    account_name:String,
    sex:Int,
    birthday:java.sql.Date,
    create_time:java.sql.Timestamp,
    last_login_time:java.sql.Timestamp,
    last_logout_time:java.sql.Timestamp,
    count_online_time:Long,
    age:Int
)

object userInfo {
    //活跃阀值
    private var ActiveThreshold = 30
    private var userInfos = Array[account_info]()
    //活跃用户
    private var activeUsers = Array[account_info]()
    
    def Init(sc:SparkContext)={
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val userModel = new userJdbc("account_info",UserJDBCurl,DtvsUser,DtvsPasswd)
        val calendar = Calendar.getInstance()
        val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
        calendar.add(Calendar.DAY_OF_YEAR, 0 - ActiveThreshold)
        val thresholdTime = ymdFormat.format(calendar.getTime)
        userModel.setCondition(Array(s"last_login_time > '$thresholdTime'"))
        activeUsers = userModel.getUserInfo(sc, sqlContext)
        printInitResult
    }
    //打印结果
    def printInitResult()={
        MyLogger.debug(s"userSize=${userInfos.size},activeUser=${activeUsers.size}")
        activeUsers.foreach(x=>MyLogger.debug(x.toString()))
    }
    def getUserInfoMap()={
        userInfos.map(x=>x.DA->x).toMap
    }
}