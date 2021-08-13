package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object ConsumedBeverages {

    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        val res1 = 
        sql("Select branch as Branch, beverage as `Most Consumed Beverage` " +
            "From allbranchcc Where Branch = 'Branch1' Group By beverage, branch Having " +
            "Sum(consumption) = (Select Max(sumc) from (Select Sum(consumption) as sumc From allbranchcc where branch = " +
            "'Branch1' Group By beverage, branch) as sc)"
        )

        val res2 =
        sql("Select branch as Branch, beverage as `Least Consumed Beverage` " +
            "From allbranchcc Where Branch = 'Branch2' Group By beverage, branch Having " +
            "Sum(consumption) = (Select Min(sumc) from (Select Sum(consumption) as sumc From allbranchcc where branch = " +
            "'Branch2' Group By beverage, branch) as sc)"
        )

        val res3 =
        sql("Select branch as Branch, beverage as `Consumed Beverage On Average` " +
            "From allbranchcc Where Branch = 'Branch2' Group By beverage, branch Having " +
            "Sum(consumption) = (Select Max(sumc) from (Select Sum(consumption) as sumc From allbranchcc where branch = " +
            "'Branch2' Group By beverage, branch) as sc)"
        )
        
        val res4 = 
        sql("Select branch as Branch, AVG(consumption) as `Average Consumption of All Beverages` " +
            "From allbranchcc where Branch = 'Branch2' Group by branch"
        )

        val res5 =
        sql("Select ro as `Row Number`, Branch, Beverage as `Beverage Median`, cons As Consumption " +
            "From (Select Row_Number() Over (Order By Sum(Consumption)) As ro, branch as Branch, beverage as Beverage, Sum(consumption) " +
            "As cons From allbranchcc Where branch = 'Branch2' Group By beverage,branch) as sub " +
            "where sub.ro = (Select Round(Count(beverage)/2,0) From (Select beverage " +
            "From allbranchcc Where branch = 'Branch2' Group By beverage,branch) as sub2)"
        )

        Seq(res1,res2,res3,res4,res5)
    }
}