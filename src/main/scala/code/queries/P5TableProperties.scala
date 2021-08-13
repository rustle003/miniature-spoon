package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object TableProperties {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        sql("Alter Table bbc unset tblproperties ('comment','note')")

        val res1 = sql("Show Tables")
        val res2 = sql("Show tblproperties bbc")
        
        sql("Alter Table bbc set tblproperties ('comment'='Table -> bbc','note'='Not the British Broadcasting Corporation')")

        val res3 = sql("Show tblproperties bbc")

        Seq(res1,res2,res3)
    }
}