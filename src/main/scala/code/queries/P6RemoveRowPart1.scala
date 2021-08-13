package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object RemoveRowP1 {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        sql("Drop Table If Exists scenario6")

        sql("set hive.support.concurrency=true")
        sql("set hive.exec.dynamic.partition.mode=nonstrict")
        sql("set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager")
        sql("set hive.compactor.initiator.on=true")
        sql("set hive.compactor.worker.threads=1")

        sql("Create Table scenario6 (beverage String, branch String) Clustered By (beverage) Into 27 Buckets Stored As Orc Tblproperties ('transactional'='true')")
        sql("Insert Into scenario6 Select * From bba")

        val res1 = sql(compareSc6Andbba)

        Seq(res1)
    }

    def compareSc6Andbba: String = {
        "Select sc6.branch As `scenario6 Branch`, " +
        "sc6.beverage as `scenario6 Beverage`, " +
        "ba.beverage as `bba Beverage`, " +
        "ba.branch as `bba Branch` " +
        "From (Select Row_Number() Over (Order By branch, beverage) as r, * From scenario6) as sc6 " +
        "Right Outer Join (Select Row_Number() Over (Order By branch, beverage) as r, * From bba) as ba " +
        "On sc6.r = ba.r"
    }
}