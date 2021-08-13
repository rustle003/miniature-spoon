package code.queries

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object NumberOfConsumers {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        val res1 = sql("Select branch as Branch, Sum(consumption) as Total From branch1conscount Group By Branch")
        val res2 = sql("Select branch as Branch, Sum(consumption) as Total from allbranchcc Group by Branch Having Branch = 'Branch2'")
        Seq(res1, res2)
    }
}