package code.conn.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object LocalHiveCon {
    private var hiveCon: HiveContext = null
    private var sparkS: SparkSession = null

    def apply(): HiveContext = if (hiveCon == null) {hiveCon = configureHiveContext; hiveCon} else hiveCon
    def hiveEnabledSparkSession: SparkSession = if (sparkS == null) {hiveCon = configureHiveContext; sparkS} else sparkS

    private def configureHiveContext: HiveContext = {
        val master  = "local[4]"
        val appName = "project_01"
        val spHome  = "/home/rustle/.local/lib/hadoop/spark-3.1.2"
        val jarPath = "file:///home/rustle/.local/lib/apache-hive-3.1.2-bin/lib/*," +
                            "file:///home/rustle/.local/lib/apache-hive-3.1.2-bin/hive-metastore/hadoop-jars/*"
        val sets    = Array(
                        ("spark.sql.broadcastTimeout", "10000"),
                        ("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse"),
                        ("spark.sql.hive.metastore.version","3.1.2"),
                        ("spark.sql.hive.metastore.jars", "path"),
                        ("spark.sql.hive.metastore.jars.path", jarPath)
                    ).toIterable
        
        val logLvl  = "ERROR"

        val spConf = new SparkConf().setMaster(master).setAppName(appName).setSparkHome(spHome).setAll(sets)
        sparkS = SparkSession.builder.config(spConf).enableHiveSupport().getOrCreate()
        val spCont = sparkS.sparkContext
        spCont.setLogLevel(logLvl)
        new HiveContext(spCont)
    }

    def stop: Unit = {
        sparkS.sparkContext.cancelAllJobs()
        sparkS.sparkContext.clearCallSite()
        sparkS.close()
    }


}