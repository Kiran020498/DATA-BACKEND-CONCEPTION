Microsoft Windows [Version 10.0.22631.4602]
(c) Microsoft Corporation. All rights reserved.

C:\Users\SourceCode>cd C:\spark\bin

C:\spark\bin>spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/18 10:41:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.5.98:4040
Spark context available as 'sc' (master = local[*], app id = local-1734500474904).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.25)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@7f277cf6

scala>   .appName("KafkaConnectorTest")
res0: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@7f277cf6

scala>   .master("local[*]")
res1: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@7f277cf6

scala>   .getOrCreate()
24/12/18 10:42:05 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
res2: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@2f16c999

scala>   .format("kafka")
<console>:25: error: value format is not a member of org.apache.spark.sql.SparkSession
       res2  .format("kafka")
              ^

scala>   .option("kafka.bootstrap.servers", "localhost:9092")
<console>:25: error: value option is not a member of org.apache.spark.sql.SparkSession
       res2  .option("kafka.bootstrap.servers", "localhost:9092")
              ^

scala>   .option("subscribe", "test-topic")
<console>:25: error: value option is not a member of org.apache.spark.sql.SparkSession
       res2  .option("subscribe", "test-topic")
              ^

scala>   .load()
<console>:25: error: value load is not a member of org.apache.spark.sql.SparkSession
       res2  .load()
              ^

scala> kafkaDF.printSchema()
<console>:24: error: not found: value kafkaDF
       kafkaDF.printSchema()
       ^

scala> :quite
No such command ':quite'.  Type :help for help.

scala> :quit

C:\spark\bin>spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/18 10:43:53 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.5.98:4040
Spark context available as 'sc' (master = local[*], app id = local-1734500636278).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.25)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@68172eeb

scala>   .appName("KafkaConnectorTest")
res0: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@68172eeb

scala>   .master("local[*]")
res1: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@68172eeb

scala>   .getOrCreate()
24/12/18 10:44:29 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
res2: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@43b5274e

scala> val kafkaDF = spark.readStream
<console>:24: error: value readStream is not a member of org.apache.spark.sql.SparkSession.Builder
       val kafkaDF = spark.readStream
                           ^

scala>   .format("kafka")
<console>:25: error: value format is not a member of org.apache.spark.sql.SparkSession
       res2  .format("kafka")
              ^

scala>   .option("kafka.bootstrap.servers", "localhost:9092")
<console>:25: error: value option is not a member of org.apache.spark.sql.SparkSession
       res2  .option("kafka.bootstrap.servers", "localhost:9092")
              ^

scala>   .option("subscribe", "test-topic")
<console>:25: error: value option is not a member of org.apache.spark.sql.SparkSession
       res2  .option("subscribe", "test-topic")
              ^

scala>   .load()
<console>:25: error: value load is not a member of org.apache.spark.sql.SparkSession
       res2  .load()
              ^

scala> val spark = SparkSession.builder
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@75573368

scala>   .appName("KafkaConnectorTest")
res7: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@75573368

scala>   .master("local[*]")
res8: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@75573368

scala>   .getOrCreate()
res9: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@43b5274e

scala> val kafkaDF = spark.readStream
<console>:24: error: value readStream is not a member of org.apache.spark.sql.SparkSession.Builder
       val kafkaDF = spark.readStream
                           ^

scala> :quit

C:\spark\bin>spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/18 10:47:13 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.5.98:4040
Spark context available as 'sc' (master = local[*], app id = local-1734500834977).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.25)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession

scala> val spark = SparkSession.builder
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@6f32cfff

scala>   .appName("KafkaConnectorTest")
res0: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@6f32cfff

scala>   .master("local[*]")
res1: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@6f32cfff

scala>   .getOrCreate()
24/12/18 10:47:39 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.
res2: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@43b5a9fd

scala> println(spark.getClass)
class org.apache.spark.sql.SparkSession$Builder

scala> val kafkaDF = spark.readStream
<console>:24: error: value readStream is not a member of org.apache.spark.sql.SparkSession.Builder
       val kafkaDF = spark.readStream
                           ^

scala>   .format("kafka")
<console>:25: error: value format is not a member of Unit
       res3  .format("kafka")
              ^

scala>   .option("kafka.bootstrap.servers", "localhost:9092")
<console>:25: error: value option is not a member of Unit
       res3  .option("kafka.bootstrap.servers", "localhost:9092")
              ^

scala>   .option("subscribe", "test-topic")
<console>:25: error: value option is not a member of Unit
       res3  .option("subscribe", "test-topic")
              ^

scala>   .load()
<console>:25: error: value load is not a member of Unit
       res3  .load()
              ^

scala> spark-shell
<console>:25: error: value - is not a member of org.apache.spark.sql.SparkSession.Builder
       spark-shell
            ^
<console>:25: error: not found: value shell
       spark-shell
             ^

scala> :quit
24/12/18 10:55:24 ERROR ShutdownHookManager: Exception while deleting Spark temp dir: C:\Users\SourceCode\AppData\Local\Temp\spark-287f2dc5-f9f8-42a4-bb1b-e4bc6bf0524a\repl-61804cd1-d064-481e-9570-c56505fafd15
java.io.IOException: Failed to delete: C:\Users\SourceCode\AppData\Local\Temp\spark-287f2dc5-f9f8-42a4-bb1b-e4bc6bf0524a\repl-61804cd1-d064-481e-9570-c56505fafd15\$line19\$read.class
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:147)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:130)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:130)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:90)
        at org.apache.spark.util.SparkFileUtils.deleteRecursively(SparkFileUtils.scala:121)
        at org.apache.spark.util.SparkFileUtils.deleteRecursively$(SparkFileUtils.scala:120)
        at org.apache.spark.util.Utils$.deleteRecursively(Utils.scala:1126)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$4(ShutdownHookManager.scala:65)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$4$adapted(ShutdownHookManager.scala:62)
        at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:36)
        at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:33)
        at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:198)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$2(ShutdownHookManager.scala:62)
        at org.apache.spark.util.SparkShutdownHook.run(ShutdownHookManager.scala:214)
        at org.apache.spark.util.SparkShutdownHookManager.$anonfun$runAll$2(ShutdownHookManager.scala:188)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.util.Utils$.logUncaughtExceptions(Utils.scala:1928)
        at org.apache.spark.util.SparkShutdownHookManager.$anonfun$runAll$1(ShutdownHookManager.scala:188)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at scala.util.Try$.apply(Try.scala:213)
        at org.apache.spark.util.SparkShutdownHookManager.runAll(ShutdownHookManager.scala:188)
        at org.apache.spark.util.SparkShutdownHookManager$$anon$2.run(ShutdownHookManager.scala:178)
        at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
24/12/18 10:55:24 ERROR ShutdownHookManager: Exception while deleting Spark temp dir: C:\Users\SourceCode\AppData\Local\Temp\spark-287f2dc5-f9f8-42a4-bb1b-e4bc6bf0524a
java.io.IOException: Failed to delete: C:\Users\SourceCode\AppData\Local\Temp\spark-287f2dc5-f9f8-42a4-bb1b-e4bc6bf0524a\repl-61804cd1-d064-481e-9570-c56505fafd15\$line19\$read.class
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:147)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:130)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:130)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingJavaIO(JavaUtils.java:130)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:117)
        at org.apache.spark.network.util.JavaUtils.deleteRecursively(JavaUtils.java:90)
        at org.apache.spark.util.SparkFileUtils.deleteRecursively(SparkFileUtils.scala:121)
        at org.apache.spark.util.SparkFileUtils.deleteRecursively$(SparkFileUtils.scala:120)
        at org.apache.spark.util.Utils$.deleteRecursively(Utils.scala:1126)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$4(ShutdownHookManager.scala:65)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$4$adapted(ShutdownHookManager.scala:62)
        at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:36)
        at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:33)
        at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:198)
        at org.apache.spark.util.ShutdownHookManager$.$anonfun$new$2(ShutdownHookManager.scala:62)
        at org.apache.spark.util.SparkShutdownHook.run(ShutdownHookManager.scala:214)
        at org.apache.spark.util.SparkShutdownHookManager.$anonfun$runAll$2(ShutdownHookManager.scala:188)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.util.Utils$.logUncaughtExceptions(Utils.scala:1928)
        at org.apache.spark.util.SparkShutdownHookManager.$anonfun$runAll$1(ShutdownHookManager.scala:188)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at scala.util.Try$.apply(Try.scala:213)
        at org.apache.spark.util.SparkShutdownHookManager.runAll(ShutdownHookManager.scala:188)
        at org.apache.spark.util.SparkShutdownHookManager$$anon$2.run(ShutdownHookManager.scala:178)
        at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
        at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)

C:\spark\bin>import org.apache.spark.sql.SparkSession
'import' is not recognized as an internal or external command,
operable program or batch file.

C:\spark\bin>val staticDF = spark.read
'val' is not recognized as an internal or external command,
operable program or batch file.

C:\spark\bin>  .option("header", "true")
'.option' is not recognized as an internal or external command,
operable program or batch file.

C:\spark\bin>  .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
'.csv' is not recognized as an internal or external command,
operable program or batch file.

C:\spark\bin>
C:\spark\bin>staticDF.show()
'staticDF.show' is not recognized as an internal or external command,
operable program or batch file.

C:\spark\bin>spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/12/18 11:30:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.5.98:4040
Spark context available as 'sc' (master = local[*], app id = local-1734503423381).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.25)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val staticDF = spark.read
staticDF: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@6fc294ae

scala>   .option("header", "true")
res0: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@6fc294ae

scala>   .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
<console>:1: error: invalid escape character
       res0  .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
                      ^
<console>:1: error: invalid escape character
       res0  .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
                           ^
<console>:1: error: invalid escape character
       res0  .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
                                    ^
<console>:1: error: invalid escape character
       res0  .csv("D:\Adil\dec 2024\UC-9868\Daily Household Transactions.csv")
                                            ^

scala>

scala> staticDF.show()
<console>:24: error: value show is not a member of org.apache.spark.sql.DataFrameReader
       staticDF.show()
                ^

scala> val staticDF = spark.read
staticDF: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@53279691

scala>   .option("header", "true")
res2: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@53279691

scala>   .csv("D:\\Adil\dec 2024\\UC-9868\\Daily Household Transactions.csv")
<console>:1: error: invalid escape character
       res2  .csv("D:\\Adil\dec 2024\\UC-9868\\Daily Household Transactions.csv")
                            ^

scala>

scala> staticDF.show()val staticDF = spark.read
<console>:1: error: ';' expected but 'val' found.
       staticDF.show()val staticDF = spark.read
                      ^

scala>   .option("header", "true")
res3: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@53279691

scala>   .csv("D:\\Adil\\dec 2024\\UC-9868\\Daily Household Transactions.csv")
res4: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 6 more fields]

scala>

scala> staticDF.show()
<console>:24: error: value show is not a member of org.apache.spark.sql.DataFrameReader
       staticDF.show()
                ^

scala> val staticDF = spark.read
staticDF: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@7e1c636a

scala>   .option("header", "true")
res6: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@7e1c636a

scala>   .csv("D:\\Adil\\dec 2024\\UC-9868\\Daily Household Transactions.csv")
res7: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 6 more fields]

scala> staticDF.show()
<console>:24: error: value show is not a member of org.apache.spark.sql.DataFrameReader
       staticDF.show()
                ^

scala> val staticDF = spark.read
staticDF: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@5c15e5f7

scala>   .option("header", "true")
res9: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@5c15e5f7

scala>   .csv("D:\\Adil\\dec 2024\\UC-9868\\Daily Household Transactions.csv")
res10: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 6 more fields]

scala> staticDF.show()
<console>:24: error: value show is not a member of org.apache.spark.sql.DataFrameReader
       staticDF.show()
                ^

scala> val staticDF = spark.read
staticDF: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@4c57e410

scala>   .option("header", "true")
res12: org.apache.spark.sql.DataFrameReader = org.apache.spark.sql.DataFrameReader@4c57e410

scala>   .csv("D:\\Adil\\dec 2024\\UC-9868\\Daily Household Transactions.csv")
res13: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 6 more fields]

scala> staticDF.show()
<console>:24: error: value show is not a member of org.apache.spark.sql.DataFrameReader
       staticDF.show()
                ^

scala> val staticDF = spark.read.option("header", "true").csv("D:\\Adil\\dec 2024\\UC-9868\\Daily Household Transactions.csv")
staticDF: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 6 more fields]

scala> staticDF.show()
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
|               Date|                Mode|            Category|         Subcategory|                Note|Amount|Income/Expense|Currency|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
|20/09/2018 12:04:08|                Cash|      Transportation|               Train|2 Place 5 to Place 0|    30|       Expense|     INR|
|20/09/2018 12:03:15|                Cash|                Food|              snacks|Idli medu Vada mi...|    60|       Expense|     INR|
|         19/09/2018|Saving Bank accou...|        subscription|             Netflix|1 month subscription|   199|       Expense|     INR|
|17/09/2018 23:41:17|Saving Bank accou...|        subscription|Mobile Service Pr...|   Data booster pack|    19|       Expense|     INR|
|16/09/2018 17:15:08|                Cash|           Festivals|        Ganesh Pujan|         Ganesh idol|   251|       Expense|     INR|
|15/09/2018 06:34:17|         Credit Card|        subscription|            Tata Sky|Permanent Residen...|   200|       Expense|     INR|
|14/09/2018 05:39:17|                Cash|      Transportation|                auto|Place 2 station t...|    50|       Expense|     INR|
|13/09/2018 21:35:15|Saving Bank accou...|      Transportation|               Train|2 Place 0 to Place 3|    40|       Expense|     INR|
|13/09/2018 21:01:47|         Credit Card|               Other|                NULL|HBR 2 Months subs...|    83|       Expense|     INR|
|13/09/2018 21:01:32|                Cash|                Food|             Grocery|            1kg atta|    46|       Expense|     INR|
|         13/09/2018|Saving Bank accou...|    Small Cap fund 2|                NULL|                NULL|  5000|  Transfer-Out|     INR|
|         13/09/2018|Saving Bank accou...|    Small cap fund 1|                NULL|                NULL|  5000|  Transfer-Out|     INR|
|          12/9/2018|         Credit Card|        subscription|Mobile Service Pr...|   Data booster pack|   667|       Expense|     INR|
|          11/9/2018|Saving Bank accou...|                Food|               Lunch|  Home Food Delivery|   650|       Expense|     INR|
|          11/9/2018|Saving Bank accou...|               Other|                NULL|         From Family|  3500|        Income|     INR|
|          11/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|
|          10/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|
|           8/9/2018|                Cash|              Family|        Pocket money|                NULL|    40|       Expense|     INR|
|           7/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    37|       Expense|     INR|
|           7/9/2018|Saving Bank accou...|Equity Mutual Fund E|                NULL|                NULL|  1000|  Transfer-Out|     INR|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
only showing top 20 rows


scala> staticDF.printSchema()
root
 |-- Date: string (nullable = true)
 |-- Mode: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Subcategory: string (nullable = true)
 |-- Note: string (nullable = true)
 |-- Amount: string (nullable = true)
 |-- Income/Expense: string (nullable = true)
 |-- Currency: string (nullable = true)


scala> val cleanedDF = staticDF.filter($"Date".isNotNull && $"Amount".isNotNull)
cleanedDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Date: string, Mode: string ... 6 more fields]

scala> cleanedDF.show()
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
|               Date|                Mode|            Category|         Subcategory|                Note|Amount|Income/Expense|Currency|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
|20/09/2018 12:04:08|                Cash|      Transportation|               Train|2 Place 5 to Place 0|    30|       Expense|     INR|
|20/09/2018 12:03:15|                Cash|                Food|              snacks|Idli medu Vada mi...|    60|       Expense|     INR|
|         19/09/2018|Saving Bank accou...|        subscription|             Netflix|1 month subscription|   199|       Expense|     INR|
|17/09/2018 23:41:17|Saving Bank accou...|        subscription|Mobile Service Pr...|   Data booster pack|    19|       Expense|     INR|
|16/09/2018 17:15:08|                Cash|           Festivals|        Ganesh Pujan|         Ganesh idol|   251|       Expense|     INR|
|15/09/2018 06:34:17|         Credit Card|        subscription|            Tata Sky|Permanent Residen...|   200|       Expense|     INR|
|14/09/2018 05:39:17|                Cash|      Transportation|                auto|Place 2 station t...|    50|       Expense|     INR|
|13/09/2018 21:35:15|Saving Bank accou...|      Transportation|               Train|2 Place 0 to Place 3|    40|       Expense|     INR|
|13/09/2018 21:01:47|         Credit Card|               Other|                NULL|HBR 2 Months subs...|    83|       Expense|     INR|
|13/09/2018 21:01:32|                Cash|                Food|             Grocery|            1kg atta|    46|       Expense|     INR|
|         13/09/2018|Saving Bank accou...|    Small Cap fund 2|                NULL|                NULL|  5000|  Transfer-Out|     INR|
|         13/09/2018|Saving Bank accou...|    Small cap fund 1|                NULL|                NULL|  5000|  Transfer-Out|     INR|
|          12/9/2018|         Credit Card|        subscription|Mobile Service Pr...|   Data booster pack|   667|       Expense|     INR|
|          11/9/2018|Saving Bank accou...|                Food|               Lunch|  Home Food Delivery|   650|       Expense|     INR|
|          11/9/2018|Saving Bank accou...|               Other|                NULL|         From Family|  3500|        Income|     INR|
|          11/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|
|          10/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|
|           8/9/2018|                Cash|              Family|        Pocket money|                NULL|    40|       Expense|     INR|
|           7/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    37|       Expense|     INR|
|           7/9/2018|Saving Bank accou...|Equity Mutual Fund E|                NULL|                NULL|  1000|  Transfer-Out|     INR|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+
only showing top 20 rows


scala> cleanedDF.describe("Amount").show()
+-------+------------------+
|summary|            Amount|
+-------+------------------+
|  count|              2461|
|   mean|2751.1453799268593|
| stddev|12519.615804355946|
|    min|                10|
|    max|               999|
+-------+------------------+


scala> val categorySummary = cleanedDF.groupBy("Category").sum("Amount")
org.apache.spark.sql.AnalysisException: "Amount" is not a numeric column. Aggregation function can only be applied on a numeric column.
  at org.apache.spark.sql.errors.QueryCompilationErrors$.aggregationFunctionAppliedOnNonNumericColumnError(QueryCompilationErrors.scala:3162)
  at org.apache.spark.sql.RelationalGroupedDataset.$anonfun$aggregateNumericColumns$1(RelationalGroupedDataset.scala:102)
  at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
  at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:36)
  at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:33)
  at scala.collection.mutable.WrappedArray.foreach(WrappedArray.scala:38)
  at scala.collection.TraversableLike.map(TraversableLike.scala:286)
  at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
  at scala.collection.AbstractTraversable.map(Traversable.scala:108)
  at org.apache.spark.sql.RelationalGroupedDataset.aggregateNumericColumns(RelationalGroupedDataset.scala:99)
  at org.apache.spark.sql.RelationalGroupedDataset.sum(RelationalGroupedDataset.scala:316)
  ... 47 elided

scala> categorySummary.show()
<console>:23: error: not found: value categorySummary
       categorySummary.show()
       ^

scala> staticDF.printSchema()
root
 |-- Date: string (nullable = true)
 |-- Mode: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Subcategory: string (nullable = true)
 |-- Note: string (nullable = true)
 |-- Amount: string (nullable = true)
 |-- Income/Expense: string (nullable = true)
 |-- Currency: string (nullable = true)


scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> val numericDF = staticDF.withColumn("AmountNumeric", col("Amount").cast("Double"))
numericDF: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 7 more fields]

scala> numericDF.printSchema()
root
 |-- Date: string (nullable = true)
 |-- Mode: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Subcategory: string (nullable = true)
 |-- Note: string (nullable = true)
 |-- Amount: string (nullable = true)
 |-- Income/Expense: string (nullable = true)
 |-- Currency: string (nullable = true)
 |-- AmountNumeric: double (nullable = true)


scala> val cleanedDF = numericDF.filter($"AmountNumeric".isNotNull)
cleanedDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Date: string, Mode: string ... 7 more fields]

scala> cleanedDF.show()
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+
|               Date|                Mode|            Category|         Subcategory|                Note|Amount|Income/Expense|Currency|AmountNumeric|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+
|20/09/2018 12:04:08|                Cash|      Transportation|               Train|2 Place 5 to Place 0|    30|       Expense|     INR|         30.0|
|20/09/2018 12:03:15|                Cash|                Food|              snacks|Idli medu Vada mi...|    60|       Expense|     INR|         60.0|
|         19/09/2018|Saving Bank accou...|        subscription|             Netflix|1 month subscription|   199|       Expense|     INR|        199.0|
|17/09/2018 23:41:17|Saving Bank accou...|        subscription|Mobile Service Pr...|   Data booster pack|    19|       Expense|     INR|         19.0|
|16/09/2018 17:15:08|                Cash|           Festivals|        Ganesh Pujan|         Ganesh idol|   251|       Expense|     INR|        251.0|
|15/09/2018 06:34:17|         Credit Card|        subscription|            Tata Sky|Permanent Residen...|   200|       Expense|     INR|        200.0|
|14/09/2018 05:39:17|                Cash|      Transportation|                auto|Place 2 station t...|    50|       Expense|     INR|         50.0|
|13/09/2018 21:35:15|Saving Bank accou...|      Transportation|               Train|2 Place 0 to Place 3|    40|       Expense|     INR|         40.0|
|13/09/2018 21:01:47|         Credit Card|               Other|                NULL|HBR 2 Months subs...|    83|       Expense|     INR|         83.0|
|13/09/2018 21:01:32|                Cash|                Food|             Grocery|            1kg atta|    46|       Expense|     INR|         46.0|
|         13/09/2018|Saving Bank accou...|    Small Cap fund 2|                NULL|                NULL|  5000|  Transfer-Out|     INR|       5000.0|
|         13/09/2018|Saving Bank accou...|    Small cap fund 1|                NULL|                NULL|  5000|  Transfer-Out|     INR|       5000.0|
|          12/9/2018|         Credit Card|        subscription|Mobile Service Pr...|   Data booster pack|   667|       Expense|     INR|        667.0|
|          11/9/2018|Saving Bank accou...|                Food|               Lunch|  Home Food Delivery|   650|       Expense|     INR|        650.0|
|          11/9/2018|Saving Bank accou...|               Other|                NULL|         From Family|  3500|        Income|     INR|       3500.0|
|          11/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|         36.0|
|          10/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|         36.0|
|           8/9/2018|                Cash|              Family|        Pocket money|                NULL|    40|       Expense|     INR|         40.0|
|           7/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    37|       Expense|     INR|         37.0|
|           7/9/2018|Saving Bank accou...|Equity Mutual Fund E|                NULL|                NULL|  1000|  Transfer-Out|     INR|       1000.0|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+
only showing top 20 rows


scala> val categorySummary = cleanedDF.groupBy("Category").sum("AmountNumeric")
categorySummary: org.apache.spark.sql.DataFrame = [Category: string, sum(AmountNumeric): double]

scala> categorySummary.show()
+--------------------+------------------+
|            Category|sum(AmountNumeric)|
+--------------------+------------------+
|          Petty cash|           13170.0|
|           Education|             537.0|
|    garbage disposal|              67.0|
|           Festivals|            6911.0|
|                Food| 96403.09999999999|
|Equity Mutual Fund B|          100000.0|
|          Tax refund|           26130.0|
|Equity Mutual Fund A|           63000.0|
|   Recurring Deposit|           47000.0|
|                Rent|           10709.0|
|Equity Mutual Fund E|           71000.0|
|        Share Market|          276161.0|
|                Cook|           12443.0|
|              Health|          66252.75|
|             Apparel|          25373.82|
|             Culture| 4304.360000000001|
|         Social Life|             298.0|
|     Maturity amount|          382792.0|
|               scrap|             220.0|
|           Household|         161645.58|
+--------------------+------------------+
only showing top 20 rows


scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala>

scala> val formattedDF = cleanedDF.withColumn("FormattedDate", to_date($"Date", "dd/MM/yyyy"))
formattedDF: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 8 more fields]

scala> formattedDF.show()
24/12/18 11:42:20 ERROR Executor: Exception in task 0.0 in stage 14.0 (TID 12)
org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 20 more
24/12/18 11:42:20 WARN TaskSetManager: Lost task 0.0 in stage 14.0 (TID 12) (192.168.5.98 executor driver): org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 20 more

24/12/18 11:42:20 ERROR TaskSetManager: Task 0 in stage 14.0 failed 1 times; aborting job
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 14.0 failed 1 times, most recent failure: Lost task 0.0 in stage 14.0 (TID 12) (192.168.5.98 executor driver): org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 20 more

Driver stacktrace:
  at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
  at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
  at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
  at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
  at scala.Option.foreach(Option.scala:407)
  at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
  at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2414)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2433)
  at org.apache.spark.sql.execution.SparkPlan.executeTake(SparkPlan.scala:530)
  at org.apache.spark.sql.execution.SparkPlan.executeTake(SparkPlan.scala:483)
  at org.apache.spark.sql.execution.CollectLimitExec.executeCollect(limit.scala:61)
  at org.apache.spark.sql.Dataset.collectFromPlan(Dataset.scala:4333)
  at org.apache.spark.sql.Dataset.$anonfun$head$1(Dataset.scala:3316)
  at org.apache.spark.sql.Dataset.$anonfun$withAction$2(Dataset.scala:4323)
  at org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:546)
  at org.apache.spark.sql.Dataset.$anonfun$withAction$1(Dataset.scala:4321)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
  at org.apache.spark.sql.Dataset.withAction(Dataset.scala:4321)
  at org.apache.spark.sql.Dataset.head(Dataset.scala:3316)
  at org.apache.spark.sql.Dataset.take(Dataset.scala:3539)
  at org.apache.spark.sql.Dataset.getRows(Dataset.scala:280)
  at org.apache.spark.sql.Dataset.showString(Dataset.scala:315)
  at org.apache.spark.sql.Dataset.show(Dataset.scala:838)
  at org.apache.spark.sql.Dataset.show(Dataset.scala:797)
  at org.apache.spark.sql.Dataset.show(Dataset.scala:806)
  ... 51 elided
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
  at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
  at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
  at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
  at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
  at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
  at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
  at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
  at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
  at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
  at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
  at org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
  at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
  at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
  at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
  at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
  at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
  at org.apache.spark.scheduler.Task.run(Task.scala:141)
  at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
  at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
  at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
  at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
  at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
  at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
  at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
  at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
  at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
  ... 20 more

scala> formattedDF.write.option("header", "true").csv("D:\\CleanedData.csv")
24/12/18 11:42:31 ERROR Utils: Aborting task
org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more
24/12/18 11:42:31 ERROR FileFormatWriter: Job job_202412181142313250267225571867797_0015 aborted.
24/12/18 11:42:31 ERROR Executor: Exception in task 0.0 in stage 15.0 (TID 13)
org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
        at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more
24/12/18 11:42:31 WARN TaskSetManager: Lost task 0.0 in stage 15.0 (TID 13) (192.168.5.98 executor driver): org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
        at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more

24/12/18 11:42:31 ERROR TaskSetManager: Task 0 in stage 15.0 failed 1 times; aborting job
24/12/18 11:42:31 ERROR FileFormatWriter: Aborting job bd724efa-9a72-4755-beba-5f8a46f2752c.
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 15.0 failed 1 times, most recent failure: Lost task 0.0 in stage 15.0 (TID 13) (192.168.5.98 executor driver): org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
        at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
        at scala.Option.foreach(Option.scala:407)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeWrite$4(FileFormatWriter.scala:307)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:271)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
        at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
        at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
        at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
        at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:869)
        at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:391)
        at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:364)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:243)
        at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:860)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:30)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:34)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:36)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:38)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:40)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:42)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:44)
        at $line57.$read$$iw$$iw$$iw$$iw$$iw.<init>(<console>:46)
        at $line57.$read$$iw$$iw$$iw$$iw.<init>(<console>:48)
        at $line57.$read$$iw$$iw$$iw.<init>(<console>:50)
        at $line57.$read$$iw$$iw.<init>(<console>:52)
        at $line57.$read$$iw.<init>(<console>:54)
        at $line57.$read.<init>(<console>:56)
        at $line57.$read$.<init>(<console>:60)
        at $line57.$read$.<clinit>(<console>)
        at $line57.$eval$.$print$lzycompute(<console>:7)
        at $line57.$eval$.$print(<console>:6)
        at $line57.$eval.$print(<console>)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:747)
        at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1020)
        at scala.tools.nsc.interpreter.IMain.$anonfun$interpret$1(IMain.scala:568)
        at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:36)
        at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:116)
        at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:41)
        at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:567)
        at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:594)
        at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:564)
        at scala.tools.nsc.interpreter.ILoop.interpretStartingWith(ILoop.scala:865)
        at scala.tools.nsc.interpreter.ILoop.command(ILoop.scala:733)
        at scala.tools.nsc.interpreter.ILoop.processLine(ILoop.scala:435)
        at scala.tools.nsc.interpreter.ILoop.loop(ILoop.scala:456)
        at org.apache.spark.repl.SparkILoop.process(SparkILoop.scala:239)
        at org.apache.spark.repl.Main$.doMain(Main.scala:78)
        at org.apache.spark.repl.Main$.main(Main.scala:58)
        at org.apache.spark.repl.Main.main(Main.scala)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1029)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:194)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:217)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1120)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1129)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
        at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 15.0 failed 1 times, most recent failure: Lost task 0.0 in stage 15.0 (TID 13) (192.168.5.98 executor driver): org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
        at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
        at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
        at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
        at org.apache.spark.scheduler.Task.run(Task.scala:141)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
        at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
        at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
        at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
        at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
        at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
        at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
        at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
        at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
        at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
        at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
        at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
        ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
        at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
        at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
        at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
        ... 24 more

Driver stacktrace:
  at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
  at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
  at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
  at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
  at scala.Option.foreach(Option.scala:407)
  at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
  at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2393)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeWrite$4(FileFormatWriter.scala:307)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:271)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
  at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
  at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:869)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:391)
  at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:364)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:243)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:860)
  ... 51 elided
Caused by: org.apache.spark.SparkException: [TASK_WRITE_FAILED] Task failed while writing rows to file:/D:/CleanedData.csv.
  at org.apache.spark.sql.errors.QueryExecutionErrors$.taskFailedWhileWritingRowsError(QueryExecutionErrors.scala:775)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:420)
  at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
  at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
  at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
  at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
  at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
  at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
  at org.apache.spark.scheduler.Task.run(Task.scala:141)
  at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
  at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
  at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
  at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
  at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
  at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: org.apache.spark.SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
Fail to parse '20/09/2018 12:04:08' in the new parser. You can set "spark.sql.legacy.timeParserPolicy" to "LEGACY" to restore the behavior before Spark 3.0, or set to "CORRECTED" and treat it as an invalid datetime string.
  at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError(ExecutionErrors.scala:54)
  at org.apache.spark.sql.errors.ExecutionErrors.failToParseDateTimeInNewParserError$(ExecutionErrors.scala:48)
  at org.apache.spark.sql.errors.ExecutionErrors$.failToParseDateTimeInNewParserError(ExecutionErrors.scala:218)
  at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:142)
  at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:135)
  at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38)
  at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:195)
  at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(Unknown Source)
  at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)
  at org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)
  at org.apache.spark.sql.execution.datasources.FileFormatDataWriter.writeWithIterator(FileFormatDataWriter.scala:91)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeTask$1(FileFormatWriter.scala:403)
  at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1397)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:410)
  ... 17 more
Caused by: java.time.format.DateTimeParseException: Text '20/09/2018 12:04:08' could not be parsed, unparsed text found at index 10
  at java.base/java.time.format.DateTimeFormatter.parseResolved0(DateTimeFormatter.java:2049)
  at java.base/java.time.format.DateTimeFormatter.parse(DateTimeFormatter.java:1874)
  at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:193)
  ... 24 more

scala> spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

scala> val formattedDF = cleanedDF.withColumn("FormattedDate", to_date($"Date", "dd/MM/yyyy"))
formattedDF: org.apache.spark.sql.DataFrame = [Date: string, Mode: string ... 8 more fields]

scala> formattedDF.show()
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+-------------+
|               Date|                Mode|            Category|         Subcategory|                Note|Amount|Income/Expense|Currency|AmountNumeric|FormattedDate|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+-------------+
|20/09/2018 12:04:08|                Cash|      Transportation|               Train|2 Place 5 to Place 0|    30|       Expense|     INR|         30.0|   2018-09-20|
|20/09/2018 12:03:15|                Cash|                Food|              snacks|Idli medu Vada mi...|    60|       Expense|     INR|         60.0|   2018-09-20|
|         19/09/2018|Saving Bank accou...|        subscription|             Netflix|1 month subscription|   199|       Expense|     INR|        199.0|   2018-09-19|
|17/09/2018 23:41:17|Saving Bank accou...|        subscription|Mobile Service Pr...|   Data booster pack|    19|       Expense|     INR|         19.0|   2018-09-17|
|16/09/2018 17:15:08|                Cash|           Festivals|        Ganesh Pujan|         Ganesh idol|   251|       Expense|     INR|        251.0|   2018-09-16|
|15/09/2018 06:34:17|         Credit Card|        subscription|            Tata Sky|Permanent Residen...|   200|       Expense|     INR|        200.0|   2018-09-15|
|14/09/2018 05:39:17|                Cash|      Transportation|                auto|Place 2 station t...|    50|       Expense|     INR|         50.0|   2018-09-14|
|13/09/2018 21:35:15|Saving Bank accou...|      Transportation|               Train|2 Place 0 to Place 3|    40|       Expense|     INR|         40.0|   2018-09-13|
|13/09/2018 21:01:47|         Credit Card|               Other|                NULL|HBR 2 Months subs...|    83|       Expense|     INR|         83.0|   2018-09-13|
|13/09/2018 21:01:32|                Cash|                Food|             Grocery|            1kg atta|    46|       Expense|     INR|         46.0|   2018-09-13|
|         13/09/2018|Saving Bank accou...|    Small Cap fund 2|                NULL|                NULL|  5000|  Transfer-Out|     INR|       5000.0|   2018-09-13|
|         13/09/2018|Saving Bank accou...|    Small cap fund 1|                NULL|                NULL|  5000|  Transfer-Out|     INR|       5000.0|   2018-09-13|
|          12/9/2018|         Credit Card|        subscription|Mobile Service Pr...|   Data booster pack|   667|       Expense|     INR|        667.0|   2018-09-12|
|          11/9/2018|Saving Bank accou...|                Food|               Lunch|  Home Food Delivery|   650|       Expense|     INR|        650.0|   2018-09-11|
|          11/9/2018|Saving Bank accou...|               Other|                NULL|         From Family|  3500|        Income|     INR|       3500.0|   2018-09-11|
|          11/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|         36.0|   2018-09-11|
|          10/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    36|       Expense|     INR|         36.0|   2018-09-10|
|           8/9/2018|                Cash|              Family|        Pocket money|                NULL|    40|       Expense|     INR|         40.0|   2018-09-08|
|           7/9/2018|                Cash|                Food|                Milk|      Half lit  milk|    37|       Expense|     INR|         37.0|   2018-09-07|
|           7/9/2018|Saving Bank accou...|Equity Mutual Fund E|                NULL|                NULL|  1000|  Transfer-Out|     INR|       1000.0|   2018-09-07|
+-------------------+--------------------+--------------------+--------------------+--------------------+------+--------------+--------+-------------+-------------+
only showing top 20 rows


scala> formattedDF.write.option("header", "true").csv("D:\\CleanedData.csv")
org.apache.spark.sql.AnalysisException: [PATH_ALREADY_EXISTS] Path file:/D:/CleanedData.csv already exists. Set mode as "overwrite" to overwrite the existing path.
  at org.apache.spark.sql.errors.QueryCompilationErrors$.outputPathAlreadyExistsError(QueryCompilationErrors.scala:1665)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:123)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
  at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
  at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:869)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:391)
  at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:364)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:243)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:860)
  ... 51 elided

scala> formattedDF.write.option("header", "true").csv("D:\\finalCleanedData.csv")
24/12/18 11:46:13 ERROR FileFormatWriter: Aborting job b7103936-8b5b-4ed6-8ceb-9340d3dbdc3d.
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
        at org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method)
        at org.apache.hadoop.io.nativeio.NativeIO$Windows.access(NativeIO.java:793)
        at org.apache.hadoop.fs.FileUtil.canRead(FileUtil.java:1249)
        at org.apache.hadoop.fs.FileUtil.list(FileUtil.java:1454)
        at org.apache.hadoop.fs.RawLocalFileSystem.listStatus(RawLocalFileSystem.java:601)
        at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)
        at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)
        at org.apache.hadoop.fs.ChecksumFileSystem.listStatus(ChecksumFileSystem.java:761)
        at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)
        at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)
        at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.getAllCommittedTaskPaths(FileOutputCommitter.java:334)
        at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJobInternal(FileOutputCommitter.java:404)
        at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJob(FileOutputCommitter.java:377)
        at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.commitJob(HadoopMapReduceCommitProtocol.scala:192)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$writeAndCommit$3(FileFormatWriter.scala:275)
        at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
        at org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:552)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:275)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
        at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
        at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
        at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
        at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
        at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
        at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:869)
        at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:391)
        at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:364)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:243)
        at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:860)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:30)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:34)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:36)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:38)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:40)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:42)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:44)
        at $line66.$read$$iw$$iw$$iw$$iw$$iw.<init>(<console>:46)
        at $line66.$read$$iw$$iw$$iw$$iw.<init>(<console>:48)
        at $line66.$read$$iw$$iw$$iw.<init>(<console>:50)
        at $line66.$read$$iw$$iw.<init>(<console>:52)
        at $line66.$read$$iw.<init>(<console>:54)
        at $line66.$read.<init>(<console>:56)
        at $line66.$read$.<init>(<console>:60)
        at $line66.$read$.<clinit>(<console>)
        at $line66.$eval$.$print$lzycompute(<console>:7)
        at $line66.$eval$.$print(<console>:6)
        at $line66.$eval.$print(<console>)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:747)
        at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1020)
        at scala.tools.nsc.interpreter.IMain.$anonfun$interpret$1(IMain.scala:568)
        at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:36)
        at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:116)
        at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:41)
        at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:567)
        at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:594)
        at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:564)
        at scala.tools.nsc.interpreter.ILoop.interpretStartingWith(ILoop.scala:865)
        at scala.tools.nsc.interpreter.ILoop.command(ILoop.scala:733)
        at scala.tools.nsc.interpreter.ILoop.processLine(ILoop.scala:435)
        at scala.tools.nsc.interpreter.ILoop.loop(ILoop.scala:456)
        at org.apache.spark.repl.SparkILoop.process(SparkILoop.scala:239)
        at org.apache.spark.repl.Main$.doMain(Main.scala:78)
        at org.apache.spark.repl.Main$.main(Main.scala:58)
        at org.apache.spark.repl.Main.main(Main.scala)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.base/java.lang.reflect.Method.invoke(Method.java:566)
        at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
        at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1029)
        at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:194)
        at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:217)
        at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
        at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1120)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1129)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
  at org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Native Method)
  at org.apache.hadoop.io.nativeio.NativeIO$Windows.access(NativeIO.java:793)
  at org.apache.hadoop.fs.FileUtil.canRead(FileUtil.java:1249)
  at org.apache.hadoop.fs.FileUtil.list(FileUtil.java:1454)
  at org.apache.hadoop.fs.RawLocalFileSystem.listStatus(RawLocalFileSystem.java:601)
  at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)
  at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)
  at org.apache.hadoop.fs.ChecksumFileSystem.listStatus(ChecksumFileSystem.java:761)
  at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:1972)
  at org.apache.hadoop.fs.FileSystem.listStatus(FileSystem.java:2014)
  at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.getAllCommittedTaskPaths(FileOutputCommitter.java:334)
  at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJobInternal(FileOutputCommitter.java:404)
  at org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.commitJob(FileOutputCommitter.java:377)
  at org.apache.spark.internal.io.HadoopMapReduceCommitProtocol.commitJob(HadoopMapReduceCommitProtocol.scala:192)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$writeAndCommit$3(FileFormatWriter.scala:275)
  at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
  at org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:552)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:275)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
  at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
  at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:869)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:391)
  at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:364)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:243)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:860)
  ... 51 elided

scala> docker run -d \
<console>:29: error: not found: value docker
       docker run -d \
       ^
<console>:29: error: not found: value d
       docker run -d \
                   ^

scala>   --name zookeeper \
<console>:29: error: not found: value --
         --name zookeeper \
         ^
<console>:29: error: not found: value zookeeper
         --name zookeeper \
                ^

scala>   -p 2181:2181 \
<console>:1: error: ';' expected but integer literal found.
         -p 2181:2181 \
            ^

scala>   -e ZOOKEEPER_CLIENT_PORT=2181 \
<console>:28: error: value ZOOKEEPER_CLIENT_PORT is not a member of org.apache.spark.sql.Column
         -e ZOOKEEPER_CLIENT_PORT=2181 \
            ^
<console>:29: error: value ZOOKEEPER_CLIENT_PORT is not a member of org.apache.spark.sql.Column
       val $ires6 = e.unary_$minus.ZOOKEEPER_CLIENT_PORT
                                   ^

scala>   confluentinc/cp-zookeeper
<console>:29: error: not found: value confluentinc
         confluentinc/cp-zookeeper
         ^
<console>:29: error: not found: value cp
         confluentinc/cp-zookeeper
                      ^
<console>:29: error: not found: value zookeeper
         confluentinc/cp-zookeeper
                         ^

scala> docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
<console>:1: error: ';' expected but integer literal found.
       docker run -d --name zookeeper -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper
                                         ^
