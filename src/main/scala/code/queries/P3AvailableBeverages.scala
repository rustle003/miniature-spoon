package code.queries

import code.conn.util.LocalHiveCon
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object AvailableBeverages {
    def queries: Seq[DataFrame] = {
        val lhCon = code.conn.util.LocalHiveCon()

        import lhCon.sql
        import lhCon.implicits._

        sql("use coffeedb")

        val res1 = sql(scenarioPart1)
        val res2 = sql(scenarioPart2)
        
        Seq(res1,res2)
    }

    def scenarioPart1: String = {
        "Select b10.beverage as `Branch 10 Beverages`, " +
        "b8.beverage as `Branch 8 Beverages`, " +
        "b1.beverage As `Branch 1 Beverages` " +
        "From (Select Row_Number() Over (Order By beverage) As r, beverage " +
            "From allbranches Where branch = 'Branch10' Group By beverage) As b10 " +
        "Right Outer Join (Select Row_Number() Over (Order By beverage) as r, beverage " +
            "From allbranches Where branch = 'Branch8' Group By beverage) As b8 " +
        "On b10.r = b8.r " +
        "Full Outer Join (Select Row_Number() Over (Order By beverage) as r, beverage " +
            "From allbranches Where branch = 'Branch1' Group By beverage) As b1 " +
        "On b8.r = b1.r"
    }

    def scenarioPart2: String = {
        "Select b4.branch As `Left Branch`, b4.beverage As `Shared Beverages`, b7.branch As `Right Branch` " +
        "From (Select branch, beverage from allbranches Where branch = 'Branch4' Group By rollup(branch, beverage)) as b4 " +
        "Inner Join (Select branch, beverage From allbranches Where branch = 'Branch7' Group By rollup(branch, beverage)) as b7 " +
        "On b4.beverage = b7.beverage " +
        "Order By b4.beverage"
    }
}