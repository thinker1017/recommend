



package homed.config {
  object ReConfig {

    val config = scala.xml.XML.loadFile("configRecommendSystem.xml")
    val IlogHost = (config \ "ilogHost").text
    val IlogJDBCUrl = "jdbc:mysql://%s/homed_ilog".format(IlogHost)
    val IlogUser = (config \ "ilogUser").text
    val IlogPasswd = (config \ "ilogPasswd").text

    val DtvsHost = (config \ "dtvsHost").text
    val DtvsJDBCUrl = "jdbc:mysql://%s:3306/homed_dtvs".format(DtvsHost)
    val DtvsUser = (config \ "dtvsUser").text
    val DtvsPasswd = (config \ "dtvsPasswd").text
    
    val DataHost = (config \ "dataHost").text
    val DataJDBCUrl = "jdbc:mysql://%s:3306/homed_data".format(DataHost)
    val DataUser = (config \ "dataUser").text
    val DataPasswd = (config \ "dataPasswd").text
    
    val ReportJDBCrl = "jdbc:mysql://%s:3306/reportdb".format(DtvsHost)
    
    val UserJDBCurl = "jdbc:mysql://%s:3306/homed_iusm".format(DtvsHost)
    
    val AppJDBCurl = "jdbc:mysql://%s:3306/homed_icore".format(DtvsHost)
    
    
    val PROGRAM_MOSAIC_SET_ID_BEGIN = 60000000L
    val PROGRAM_MOSAIC_SET_ID_END = 69999999L
    val PROGRAM_VIDEO_ID_BEGIN = 100000000L
    val PROGRAM_VIDEO_ID_END = 199999999L
    val PROGRAM_EVENT_ID_BEGIN = 200000000L
    val PROGRAM_EVENT_ID_END = 299999999L
    val PROGRAM_SERIES_VIDEO_ID_BEGIN = 300000000L
    val PROGRAM_SERIES_VIDEO_ID_END = 399999999L
    val PROGRAM_SERIES_EVENT_ID_BEGIN = 400000000L
    val PROGRAM_SERIES_EVENT_ID_END = 499999999L
    val PROGRAM_MUSIC_ID_BEGIN = 500000000L
    val PROGRAM_MUSIC_ID_END = 549999999L
    val PROGRAM_MUSIC_SINGER_ID_BEGIN = 550000000L
    val PROGRAM_MUSIC_SINGER_ID_END = 574999999L
    val PROGRAM_MUSIC_ALBUM_ID_BEGIN = 575000000L
    val PROGRAM_MUSIC_ALBUM_ID_END = 599999999L
    val PROGRAM_NEWS_ID_BEGIN = 600000000L
    val PROGRAM_NEWS_ID_END = 699999999L
    val PROGRAM_APP_ID_BEGIN = 1000000000L
    val PROGRAM_APP_ID_END = 1099999999L
    val PROGRAM_CHANNEL_BEGIN = 4200000000L
    val PROGRAM_CHANNEL_END = 4201999999L
    val PROGRAM_MONITOR_BEGIN = 4202000000L
    val PROGRAM_MONITOR_END = 4203999999L
    val PROGRAM_MOSAIC_CHANNEL_BEGIN = 4204000000L
    val PROGRAM_MOSAIC_CHANNEL_END = 4205999999L
    val PROGRAM_LOCAL_CHANNEL_BEGIN = 4206000000L
    val PROGRAM_LOCAL_CHANNEL_END = 4207999999L

    val PROGRAM_NUM_LIMIT = (config \ "MaxNumPerNode").text.toInt

    val REDIS_HOST = (config \ "redisHost").head.child.filter( n => n.label(0) == 'h').map( a => a.text)
    val REDIS_POST = (config \ "redisHost").head.child.filter( n => n.label(0) == 'p').map( a => a.text.toInt)
    
    val CALC_tagLE = (config \ "dataId").head.child.filter( n => n.label(0) == 'h').map( a => a.text.toInt)

    val ilogslaveRunLogPrefix = (config \ "ilogslaveRunLogPrefix").text
    
    val ilogslaveRddSavePrefix = (config \ "ilogslaveRddSavePrefix").text
   
    val SAVEDDATAINDEX = (config \ "ilogslaveSavedIndex").text
    
    val SAVINGDATAINDEX = (config \ "ilogslaveSavingIndex").text
    
    val BACKUPFILEDATA = (config \ "filebackupdate").text
    
    val SAVINGFILEDATA = (config \ "filesavingdate").text
    
    val TimeWeight = (config \ "timeWeight").text.toDouble
    
    val PlayWeight = (config \ "timeWeight").text.toDouble
    
    val LiveWeight = (config \ "LiveWeight").text.toDouble
    
    val EventWeight = (config \ "EventWeight").text.toDouble
    
    val calcEitTomorrow  = (config \ "calcEitTomorrow").text.toInt
    
    val dataTest  = (config \ "dataTest").text.toInt
    
    val SLAVE_HOST = (config \ "slave").head.child.filter( n => n.label(0) == 'h').map( a => a.text)
  }
}
