package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object RemoveRowP2 {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        sql("Drop Table If Exists scenario6")
        
        sql("Create Table scenario6 (beverage String, branch String) Clustered By (beverage) Into 27 Buckets Stored As Orc Tblproperties ('transactional'='true')")
        sql("Insert Into scenario6 Select * from bba Where bba.branch <> 'Branch9' or bba.beverage <> 'Triple Cappuccino'")

        val res1 = sql(RemoveRowP1.compareSc6Andbba)

        Seq(res1)
    }
}