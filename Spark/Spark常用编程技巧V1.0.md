# Spark 常用编程技巧

Spark Version : 2.2.0

## 1. 基础篇

### 1.1 Scala 篇

1.   求均值
    
描述： 求一个Double类型的均值，含有NaN类型（去掉NaN后求均值）；
```
scala> val data = Array(1,2,3.0)
data: Array[Double] = Array(1.0, 2.0, 3.0)

scala> def sum_num(arr:Array[Double]) = arr.foldLeft((0.0,0))((acc,elem) => if(elem.equals(Double.NaN)) acc else (acc._1+ elem,acc._2+1))
sum_num: (arr: Array[Double])(Double, Int)

scala> def avg(arr:Array[Double]) :Double= {
     |     val (sum , num) = sum_num(arr)
     |     sum/num
     |   }
avg: (arr: Array[Double])Double

scala> avg(data)
res51: Double = 2.0

scala> val data1 = Array(1,2,3.0,Double.NaN)
data1: Array[Double] = Array(1.0, 2.0, 3.0, NaN)

scala> avg(data1)
res52: Double = 2.0
```
使用 ***foldLeft*** 实现
2.   

### 1.2 Spark 篇

0. 生成 DataFrame

```
scala> case class P(name:String,age:Int,salary:Double)
defined class P

scala> val data = sc.makeRDD(Array(P("tom",23,19888),P("kate",56,2300))).toDF
data: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

scala> data.show
+----+---+-------+
|name|age| salary|
+----+---+-------+
| tom| 23|19888.0|
|kate| 56| 2300.0|
+----+---+-------+

```

1. 字符串截取
    
描述： 针对DataFrame的某个字符串字段，截取其中的某一段
```
scala> case class A(name:String)
defined class A

scala> val data = sc.makeRDD(Array(A("123456"),A("abcdef"))).toDF
data: org.apache.spark.sql.DataFrame = [name: string]

scala> data.select(substring(col("name") ,0 , 3)).show
+---------------------+                                             
|substring(name, 0, 3)|
+---------------------+
|                  123|
|                  abc|
+---------------------+

scala> data.select(substring(col("name") ,1 , 3)).show
+---------------------+
|substring(name, 1, 3)|
+---------------------+
|                  123|
|                  abc|
+---------------------+

```
***substring***(列名，开始值，截取长度)， 其中开始值其实是从 1 开始的，所以写 0 和 1 的结果是一样的；
2. DataFarme collect后，获取其中的值
```
scala> case class P(name:String,age:Int,salary:Double)
defined class P

scala> val data = sc.makeRDD(Array(P("tom",23,19888),P("kate",56,2300))).toDF
data: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

scala> data.collect
res62: Array[org.apache.spark.sql.Row] = Array([tom,23,19888.0], [kate,56,2300.0])

scala> data.collect.map(row => (row.getString(0),row.getInt(1),row.getDouble(2)))
res61: Array[(String, Int, Double)] = Array((tom,23,19888.0), (kate,56,2300.0))

scala> data.collect.map(row => (row.getString(0),row.getInt(1),row.getDouble(2))).foreach(println(_))
(tom,23,19888.0)
(kate,56,2300.0)

```
DataFrame 通过Action后，得到的是 Array[Row] 类型，Row 类型获取值需要通过 getXXX() 的形式来获得，而 XXX 对应的就是其类型，如 Double 类型，那么就是 getDouble() ;
3. 自定义udf，处理基本类型列

这里的基本类型是指：Double，String，Int，Float。。。
```
scala> case class P(name:String,age:Int,salary:Double)
defined class P

scala> val data = sc.makeRDD(Array(P("tom",23,19888),P("kate",56,2300))).toDF

scala> val handle_cols = udf {(name:String,age:Int,salary:Double) => name+"_"+age+"_" + salary} 
handle_cols: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function3>,StringType,Some(List(StringType, IntegerType, DoubleType)))

scala> data.select(handle_cols(col("name"),col("age"),col("salary"))).show()
+-------------------------+
|UDF(name, age, salary)|
+-------------------------+
|           tom_23_19888.0|
|           kate_56_2300.0|
+-------------------------+

```
4. 自定义udf，处理基本类型列时，传入固定值（数据参考3）
```
scala> val handle_cols = udf {(name:String,age:Int,salary:Double,splitter:String) => name+splitter+age+splitter + salary} 
handle_cols: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function4>,StringType,Some(List(StringType, IntegerType, DoubleType,StringType)))

scala> data.select(handle_cols(col("name"),col("age"),col("salary"),lit("_"))).show()
+-------------------------+
|UDF(name, age, salary, _)|
+-------------------------+
|           tom_23_19888.0|
|           kate_56_2300.0|
+-------------------------+
```
出入固定值，使用 ***lit*** 函数
5. 自定义udf后，新列名重命名(数据参考4)
```

scala> data.select(handle_cols(col("name"),col("age"),col("salary"),lit("_")) as "all").show()
+--------------+
|           all|
+--------------+
|tom_23_19888.0|
|kate_56_2300.0|
+--------------+

```
重命名使用 ***as*** 函数
6. 按照某个列进行分组，获取分组后的数据（Array类型）
```
scala> case class YV(y:String, v:Double)
defined class YV

scala> val data = sc.makeRDD(Array(YV("2018",1.0),YV("2018",5.0),YV("2018",4.0),YV("2019",1.0),YV("2019",3.0))).toDFdata: org.apache.spark.sql.DataFrame = [y: string, v: double]

scala> data.show()
+----+---+
|   y|  v|
+----+---+
|2018|1.0|
|2018|5.0|
|2018|4.0|
|2019|1.0|
|2019|3.0|
+----+---+
scala> data.groupBy("y").agg(collect_list(col("v")))
res66: org.apache.spark.sql.DataFrame = [y: string, collect_list(v): array<double>]

scala> data.groupBy("y").agg(collect_list(col("v"))).show
+----+---------------+
|   y|collect_list(v)|
+----+---------------+
|2019|     [1.0, 3.0]|
|2018|[1.0, 5.0, 4.0]|
+----+---------------+
```
7. 针对DataFrame的某个数组类型Flatten为基本类型（数据使用6.）
```
scala> data.groupBy("y").agg(collect_list(col("v")) as "v_list").withColumn("v_list",explode(col("v_list")))
res72: org.apache.spark.sql.DataFrame = [y: string, v_list: double]

scala> data.groupBy("y").agg(collect_list(col("v")) as "v_list").withColumn("v_list",explode(col("v_list"))).show
+----+------+
|   y|v_list|
+----+------+
|2019|   1.0|
|2019|   3.0|
|2018|   1.0|
|2018|   5.0|
|2018|   4.0|
+----+------+
```
8. 


## 2. 进阶篇

### 2.1 Spark篇
1. 自定义udf，对数组类型进行处理
```
scala> case class YV(y:String, v:Double)
defined class YV

scala> val data = sc.makeRDD(Array(YV("2018",1.0),YV("2018",5.0),YV("2018",4.0),YV("2019",1.0),YV("2019",3.0))).toDFdata: org.apache.spark.sql.DataFrame = [y: string, v: double]

scala> val handle_arr= udf{(arr:scala.collection.mutable.WrappedArray[Double]) => arr.sorted}
handle_arr: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,ArrayType(DoubleType,false),Some(List(ArrayType(DoubleType,false))))

scala> data.groupBy("y").agg(collect_list(col("v")) as "v_list").show
+----+---------------+
|   y|         v_list|
+----+---------------+
|2019|     [1.0, 3.0]|
|2018|[1.0, 5.0, 4.0]|
+----+---------------+

scala> data.groupBy("y").agg(collect_list(col("v")) as "v_list").withColumn("v_list", handle_arr(col("v_list")))
res69: org.apache.spark.sql.DataFrame = [y: string, v_list: array<double>]

scala> data.groupBy("y").agg(collect_list(col("v")) as "v_list").withColumn("v_list", handle_arr(col("v_list"))).show()
+----+---------------+
|   y|         v_list|
+----+---------------+
|2019|     [1.0, 3.0]|
|2018|[1.0, 4.0, 5.0]|
+----+---------------+

```
这里的 ***v_list*** 列就是数组类型，如果自定义函数处理这个列，那么就需要把 udf 的类型定义为： ***scala.collection.mutable.WrappedArray[Double]***
2. 合并基本数据类型为struct类型
```
scala> case class YVV(y:String, v1:Double,v2:Double)
defined class YVV

scala> val data = sc.makeRDD(Array(YVV("2018",1.0,2.0),YVV("2018",5.0,4.0),YVV("2018",4.0,4.5),YVV("2019",1.0,0.3),YVV("2019",3.0,9.7))).toDF
data: org.apache.spark.sql.DataFrame = [y: string, v1: double ... 1 more field]

scala> data.show
+----+---+---+
|   y| v1| v2|
+----+---+---+
|2018|1.0|2.0|
|2018|5.0|4.0|
|2018|4.0|4.5|
|2019|1.0|0.3|
|2019|3.0|9.7|
+----+---+---+
scala> data.select(col("y"),struct("v1","v2") as "v1_v2").show()
+----+---------+
|   y|    v1_v2|
+----+---------+
|2018|[1.0,2.0]|
|2018|[5.0,4.0]|
|2018|[4.0,4.5]|
|2019|[1.0,0.3]|
|2019|[3.0,9.7]|
+----+---------+

scala> data.select(col("y"),struct("v1","v2") as "v1_v2")
res76: org.apache.spark.sql.DataFrame = [y: string, v1_v2: struct<v1: double, v2: double>]

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").select(col("y"),col("v1_v2.v1"))
res77: org.apache.spark.sql.DataFrame = [y: string, v1: double]

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").select(col("y"),col("v1_v2.v1")).show
+----+---+
|   y| v1|
+----+---+
|2018|1.0|
|2018|5.0|
|2018|4.0|
|2019|1.0|
|2019|3.0|
+----+---+

```
合并两个基本数据类型使用 ***struct***函数，取得 ***struct*** 类型的数据使用 ***新合并列名.原列名*** 的方式获得；
3. 自定义udf，处理struct类型数据(数据使用2.)
```
scala> val handle_struct = udf{(x:org.apache.spark.sql.Row) => Array(x.getDouble(0),x.getDouble(1)).max}
handle_struct: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,DoubleType,None)

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").withColumn("v1_v2",handle_struct(col("v1_v2")))
res79: org.apache.spark.sql.DataFrame = [y: string, v1_v2: double]

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").withColumn("v1_v2",handle_struct(col("v1_v2"))).show
+----+-----+
|   y|v1_v2|
+----+-----+
|2018|  2.0|
|2018|  5.0|
|2018|  4.5|
|2019|  1.0|
|2019|  9.7|
+----+-----+
```
这里使用自定义函数，求 struct 类型中的数据的 v1 , v2 中的最大值
4. 自定义udf，处理Array[Struct] 类型数据（数据使用2.）
```
scala> data.select(col("y"),struct("v1","v2") as "v1_v2").groupBy("y").agg(collect_list(col("v1_v2")) as "v_list")
res81: org.apache.spark.sql.DataFrame = [y: string, v_list: array<struct<v1:double,v2:double>>]

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").groupBy("y").agg(collect_list(col("v1_v2")) as "v_list").show(false)
+----+---------------------------------+
|y   |v_list                           |
+----+---------------------------------+
|2019|[[1.0,0.3], [3.0,9.7]]           |
|2018|[[1.0,2.0], [5.0,4.0], [4.0,4.5]]|
+----+---------------------------------+

scala> val handle_arr_struct= udf{(arr:Seq[org.apache.spark.sql.Row]) => (arr.map(x=>x.getDouble(0)).max, arr.map(x=>x.getDouble(1)).max)}
handle_arr_struct: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StructType(StructField(_1,DoubleType,false), StructField(_2,DoubleType,false)),None)

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").groupBy("y").agg(collect_list(col("v1_v2")) as "v_list").withColumn("v_list",handle_arr_struct(col("v_list")))
res84: org.apache.spark.sql.DataFrame = [y: string, v_list: struct<_1: double, _2: double>]

scala> data.select(col("y"),struct("v1","v2") as "v1_v2").groupBy("y").agg(collect_list(col("v1_v2")) as "v_list").withColumn("v_list",handle_arr_struct(col("v_list"))).show()
+----+---------+
|   y|   v_list|
+----+---------+
|2019|[3.0,9.7]|
|2018|[5.0,4.5]|
+----+---------+

```
这里要注意，使用udf的参数类型需要是 Seq[Row] 类型，同时在函数里面，获取每个Row对应的值，使用 ***getXXX()*** 实现；
> 想一想上面的函数处理得到的结果是什么？
5. 
