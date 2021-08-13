package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object PartitionsAndViews {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        sql("Drop View If Exists scenario3V")
        sql("Drop Table If Exists scenario3P")

        val res1 = sql("Show Tables")
        sql("Create View If Not Exists scenario3V as " + AvailableBeverages.scenarioPart2)

        val res2 = sql("Select * From scenario3V")
        val res3 = sql("Show Tables")

        sql("Create Table scenario3P (beverage String, consumption Int) " +
            "Partitioned By (branch String) Row Format Delimited Fields Terminated by '\\t' " +
            "Stored As TextFile"
        )

        writePartitions(lhCon)

        val res4 = sql("Show Tables")
        val res5 = sql(AvailableBeverages.scenarioPart1.replaceAll("allbranches", "scenario3P"))
        
        Seq(res1,res2,res3,res4,res5)
    }

    private def writePartitions(hCon: HiveContext) {
        import hCon.sql
        import hCon.implicits._

        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch1') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch1') as b1"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch2') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch2') as b2"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch3') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch3') as b3"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch4') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch4') as b4"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch5') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch5') as b5"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch6') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch6') as b6"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch7') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch7') as b7"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch8') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch8') as b8"
        )
        sql("Insert Into Table scenario3P PARTITION (branch = 'Branch9') " +
            "Select * From (Select beverage, consumption from allbranchcc " +
            "Where branch = 'Branch9') as b9"
        )
    }
}