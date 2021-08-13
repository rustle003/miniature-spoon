# miniature-spoon

This program, miniature-spoon, is a small project written to explore interactions between a mix of scala, spark, and a coffee-themed hive database. The configurations of this project are specific to the machine on which it was written; however, anyone is welcome to change the configuration and adapt it to their machine most of which--referring to the configuration--will need to be done directly on the source file in src/main/scala/conn/util/LocalHiveCon.scala.

# About the Output

The program runs a series of DDL, DML, and DQL commands using HiveQL through the spark-hive library. 

# Sample Output

The following output is from a query that determines which beverages are available on three Cafe branches, one of which is non-existent, joins them, and orders them alphabetically.

```
+-------------------+------------------+------------------+
|Branch 10 Beverages|Branch 8 Beverages|Branch 1 Beverages|
+-------------------+------------------+------------------+
|               null|   Cold Cappuccino|        Cold Latte|
|               null|       Cold Coffee|         Cold Lite|
|               null|        Cold Latte|   Double Espresso|
|               null|         Cold Lite|      Double Mocha|
|               null| Double Cappuccino|      Icy Espresso|
|               null|     Double Coffee|         Icy Mocha|
|               null|   Double Espresso|  Large Cappuccino|
|               null|      Double Latte|    Large Espresso|
|               null|      Double Mocha|         Med Latte|
|               null|    Icy Cappuccino|         Med Mocha|
|               null|        Icy Coffee|        Mild Latte|
|               null|          Icy Lite|         Mild Lite|
|               null|  Large Cappuccino|    Small Espresso|
|               null|      Large Coffee|       Small Latte|
|               null|    Large Espresso|       Small Mocha|
|               null|       Large Mocha|Special Cappuccino|
|               null|    Med Cappuccino|    Special Coffee|
|               null|        Med Coffee|  Special Espresso|
|               null|      Med Espresso|      Triple Latte|
|               null|         Med Latte|      Triple Mocha|
|               null|         Med Mocha|              null|
|               null|   Mild Cappuccino|              null|
|               null|       Mild Coffee|              null|
|               null|     Mild Espresso|              null|
|               null|    Small Espresso|              null|
|               null|       Small Latte|              null|
|               null|        Small Lite|              null|
|               null|       Small Mocha|              null|
|               null|Special Cappuccino|              null|
|               null|  Special Espresso|              null|
|               null|     Special Latte|              null|
|               null|      Special Lite|              null|
|               null|     Special Mocha|              null|
|               null| Triple Cappuccino|              null|
|               null|     Triple Coffee|              null|
|               null|   Triple Espresso|              null|
|               null|       Triple Lite|              null|
+-------------------+------------------+------------------+
```