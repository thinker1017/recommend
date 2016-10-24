package homed.ilogslave.dataWarehouse
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
import homed.Jdbc.videoJdbc
import homed.tools._
import homed.config.ReConfig


case class video_info(
    video_id:Long,
    video_name:String,
    labels:String,
    descriptions:String,
    register_date:java.sql.Timestamp,
    direct_list:String,
    actor_list:String,
    video_time:java.sql.Timestamp,
    language:String,
    country:String,
    f_tab:String,
    f_tag:String,
    f_series_id:Long
)


case class video_serie(
    series_id:Long,
    series_name:String,
    service_id:Long,
    ttype:Long,
    labels:String,
    series_num:Int,
    actors:String,
    director:String,
    country:String,
    language:String,
    description:String,
    status:Int,
    series_year:String,
    f_edit_time:java.sql.Timestamp,
    f_tab:String,
    f_score:Int,
    f_tag:String
)

object videoSeries {
    private var VideoSeries = Array[video_serie]()
    private var videoInfos = Array[video_info]()
    def Init(sc:SparkContext)={
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val jdbcModel = new videoJdbc("video_series",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
      VideoSeries = jdbcModel.getVideoSeries(sc,sqlContext)
      jdbcModel.updateInfo("video_info",DtvsJDBCUrl,DtvsUser,DtvsPasswd)
      videoInfos = jdbcModel.getVideoInfos(sc, sqlContext)
      
      
    }
    def getVideoSeries()={
      VideoSeries
    }
    def getVideoInfos()={
      videoInfos
    }
}