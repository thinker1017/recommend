/* HomedDataAnalyser.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer
import homed.ilogslave.dataWarehouse._
import homed.Options
import homed.ilogslave.recommendAlgorithm.CB
import homed.ilogslave.Liverecommend
import homed.ilogslave.dataWarehouse.userBehavior._
import homed.ilogslave.RecommendStream
object RecommendSystem {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("homed RecommendSystem").set("spark.executor.memory","6g") 
    val sc = new SparkContext(conf)
    val stime = System.currentTimeMillis()
    val cml = Options.tool(args)
    val stage = if (cml.contains("stage")) cml("stage") else ""
      
    SystemStage.RunStage(stage, sc)
    
   // Ilogslave.calc(sc)
    
    println("task all end.. run:"+(System.currentTimeMillis() - stime)/1000.0/60+"min")
    sc.stop()
  }
}


//系统步骤管理
object SystemStage{
    
    private val stageList = List("seriesInit","eventInit","seriesTrait","seriesSim")
    
    def RunStage(stageName:String,sc:SparkContext)={
          val stime = System.currentTimeMillis()
          println("stageName:"+stageName)
          stageName match{
            //剧集数据补充，保存到hdfs
            case "seriesInit" =>{
                stage_seriesInit(sc)
            }
            //直播对应剧集，回看对应剧集，保存到hdfs
            case "eventInit"  =>{
                stage_eventInit(sc)
            }
            //剧集特征保存到hdfs
            case "seriesTrait" =>{
                stage_seriesTrait(sc)
            }
            //计算剧集相似度保存到hdfs
            case "seriesSim"  =>{
                stage_seriesSim(sc)
            }
            case "userHit"  =>{
                stage_userHit(sc)
            }
            case "liveRecommend" =>{
                stage_liveRecommend(sc)
            }
            case "test" =>{
                stage_test(sc)
            }
 
         }
         println(s"task ${stageName} end.. run:"+(System.currentTimeMillis() - stime)/1000.0/60+"min")
    }
    
    def stage_seriesInit(sc:SparkContext)={
         Live.InitSeries(sc)
    }
    def stage_eventInit(sc:SparkContext)={
         Live.InitEvent(sc)
    }
    def stage_seriesTrait(sc:SparkContext)={
        Live.findSeriesFeature(sc)
    }
    def stage_test(sc:SparkContext)={
        RecommendStream.RecommendStart(sc)
    }
    def stage_seriesSim(sc:SparkContext)={
        CB.ContentBased_itemsSimBroad(sc)
        //CB.ContentBased_itemsSim(sc)
    }
    def stage_userHit(sc:SparkContext)={
        Live.Init(sc)
        videoSeries.Init(sc)
        UserHit.getLiveHits(sc)
    }
    def stage_liveRecommend(sc:SparkContext)={
        Liverecommend.ContentBasedRecommendEx(sc)
       // Liverecommend.ItemCfRecommend(sc)
    }
   
  
  
}
