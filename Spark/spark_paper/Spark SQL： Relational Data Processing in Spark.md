# Spark SQL： Relational Data Processing in Spark
[Toc]
## Abstract
Spark SQL是Apache Spark中的一个新模块，其在Spark的函数式编程API中引入了关系型处理能力。基于在Shark上的经验，Spark SQL可以使得Spark开发者获得在关系型处理过程中才能得到的好处（如声明式的查询，存储优化），同时可以使SQL开发者可以在Spark中调用复杂的分析库（分析函数库，如机器学习）。和之前的系统对比，Spark SQL添加了两个主要功能：第一，通过声明式的DataFrame API以及过程处理代码，在关系型和过程型处理过程中增加一个更加紧密的交互方式；第二，新增了一个高度可扩展的优化器，Catalyst（此优化器通过使用Scala的函数式编程实现），使用此优化器，可以很容易的添加可组合的（可以理解为可定制）规则、控制代码生成过程、定义扩展点（extension points？拓展点？）利用Catalyst，针对现代的复杂数据分析，定制开发了很多特性（如 JSON元数据自动获取、机器学习函数调用、外部数据库联邦查询（federation？））。我们把Spark SQL看做是一个SQL-on -SPark 以及 SPark世界的一个变革，因为在保持Spark编程模型优势的通知，还提供了更多的API以及优化机制。
## Keywords
Databases；Data Warehouse； Machine Learning； Spark；Hadoop
## 1 Introduction
大数据应用一般是一个处理技术、数据源及存储格式的混合应用。早期的类似的系统，如MapReduce（提供了一个强大但是偏底层的过程化处理接口）。使用这种系统进行编程，一般比较复杂，而且如果需要得到更高的性能，那么需要用户手动优化。所以，很多新的系统更加倾向于使用大数据的关系型的接口，从而使得编码更加高效、同时用户易用。诸如Pig，Hive，Dremel和Shark就是这样的系统，通过声明式的查询来提供更多自动化的优化。

虽然关系型系统的活跃展示了用户更加倾向于编写声明式的查询，但是这种关系型的系统一般针对大数据应用提供的可用的操作是比较少的，同时还存在下面的问题。第一，用户可能需要的ETL工作需要多种数据源的支持，同时可能是半结构化或非结构化的数据，这时需要用户自己编写额外代码。第二，用户一般希望能进行更多高级的分析，比如机器学习或图处理，但是这点在关系型系统中是一个非常大的挑战。事实上，我们观察到很多数据操作都会用到关系型查询及过程化处理算法。不幸的是，对于关系型和过程型系统直到现在还没有一个系统可以同时具有这两种优势，这就使得用户只能选择其中的一个或另一个。

这篇论文主要介绍了我们引入的一个Spark系统中的新模型，Spark SQL。Spark SQL基于早前的Shark设计。而Spark SQL就可以让用户直接使用关系型和过程型处理的API，而不用二选一。

Spark SQL通过提供两个核心来整合关系型和过程型系统。第一个，Spark SQL提供一个可以处理外部数据源及Spark内置的分布式数据结构的API，叫做DataFrame。这个API和R中的data frame比较类似，但是当进行操作的时候是一种懒操作（部分代码不是立即执行，而是在执行某些操作的时候才执行，可以和Pig对比），基于这种“懒操作”，Spark 引擎就可以进行优化。第二，为了支持大数据系统中的多种数据源及算法库的操作Spark SQL引入了一个设计精妙的、可扩展的优化器，Catalyst。Catalyst使得添加数据源、优化规则、添加机器学习中的数据结构更加容易。

DataFrame API同时提供多种关系型或过程型操作。DataFrame是一个结构化记录的集合，可以使用Spakr的过程化API或关系型API（此种API支持多种优化）来进行处理。DataFrame可以从SPark RDD转换得到，所以可以整合到现有的Spark代码中。其他的Spark组件，如机器学习库，也可以使用DataFrame作为输入或作为输出。在很多一般的场合中，DataFrame会比Spark 过程型API（也就是RDD API）能取得更高的性能，同时也更加易于操作。举例来说，在DataFrame中，可以使用一个SQL来完成多个聚合操作，但是如果使用函数式API（RDD API），那么就会很复杂。同时，DataFrame存储数据时，会直接使用Columnar format（列式格式），这种格式比Java/Python 类更加紧凑。最后，和R或Python中的 data frame API不同的是，在Spark SQL中 DataFrame会使用一个关系型优化器，Catalyst（而Catalyst也是本文的重点）。

为了支持多种数据源和分析工作模式，我们设计了一个拓展的查询优化器，Catalyst。Catalyst充分利用Scala语言中的特性，比如使用模式识别来实现可组合的规则（Turing-complete language：图灵完整性语言？）。它提供了一个用于转换树的框架，这个框架可以执行分析、计划、运行时代码生成工作。通过这个框架，Catalyst就可以添加新的数据域，包括半结构化数据，如：JSON。and（“smart”data store to which one can push filters，HBASE，HBASE是一个高效的数据存储结构？因为可以添加过滤器？）以及用户自定义的域类型，如机器学习。函数式语言针对构建编译器具有很好的适应性，所以使用Scala来编写一个可拓展的优化器也就没有什么好奇怪的了。我们确实发现Catalyst能使我们更加高效、快速的在Spark SQL中增加性能，外部的开发者也可以很容易的添加这些规则，所以说Catalyst是更易用的。

在2014年发行了Spark SQL，现在它是Spark中最活跃的组件之一。在写这篇paper的时候，Spark是大数据中最活跃的开源项目，超过400个开发者。SPark SQL已经被应用在大数量级的场景中。举例来说，一个大型的互联网公司使用Spark SQL建立了一个暑假处理流，可以在一个8000节点的机器上处理、分析100PB的数据。每个单独的查询，一般都会操作数十TB的数据。另外，很多用户已经接受Spark SQL不单单是一个SQL查询引擎，而且从编程上来说，整合了过程处理这样的观念。例如，Databricks Cloud的2/3的用户，其托管的服务中运行Spark任务的都是使用SPark SQL。从效率方面看，我们发现Spark SQL在Hadoop的关系型产品中是很有竞争力的。对比传统RDD 代码，它可以取得10倍性能及更好的内存效率优势。

更一般的，我们把SPark SQL视为Spark Core API的一次重要的变革。Spark 原始的函数式编程API确实太常规了，在自动优化方面只提供了很有限的机会。Spark SQL不仅使得Spark对用户来说更易用，而且对已有的代码也可以进行优化。目前，Spark社区针对Spark SQL添加了更多API操作，如把DataFrame作为新的“ML pipeline”机器学习的标准数据表示格式，我们希望可以把这种表示扩展到Spark的其他组件，如Spark GraphX、Spark Streaming。

在文章开篇我们介绍了Spark背景以及Spark SQL要实现的目标（第二节$2）。接着，介绍了DataFrame API（第三节，$3），Catalyst 优化器（第四节，$4）以及在Catalyst之上构建的高级特性（第五节，$5）。在第六节对Spark SQL进行评估。在第7节针对在Catalyst的一些外部研究。最后，在第八节，cover 相关工作。

## Background and Goals
### 2.1 Spark Overview
Apache Spark 是一个通用的集群计算引擎，可以使用Scala，Python，Java API以及流处理、图处理、机器学习的相关库。在2010年发布后，很快就被广泛使用（这种多种语言集成的特性和DryadLINQ类似），并且是最活跃的大数据开源项目。在2014年，Spark已经拥有400位开发者，并且很多发行商在其上发布了很多版本。

Spark提供了一个函数式编程API，可以操作分布式数据集（RDDs）。每个RDD是一个在集群中被分区的Java或Python的object的集合。可以使用map、filter、reduce来操作RDD，这些函数的参数也是函数，通过这些函数（map。。。）可以把数据进行转换后发往集群的各个节点。比如，下面的Scala代码主要统计text文件中包含“ERROR”的个数。
```
lines = spark.textFile("hdfs://...")
errors = lines.filter(s => s.contains("ERROR"))
println("errors.count()")
```
上面这段代码通过读取一个HDFS文件，生成一个string类型的RDD，叫做liens。接着，使用filter进行转换，得到一个新的RDD，叫做errors。最后，执行一个count操作。

RDDs是具有容错性的，在系统中丢失的数据可以根据RDD的血缘图来进行恢复（在上面的代码中，如果有数据丢失，可以利用血缘图可以重新运行filter，来重建丢失的分区数据）。当然RDD也可以显式的缓存到内存或硬盘上，以此来支持循环操作。

最后一个关于API的点是RDD执行时是lazy的（先构建Graph，然后当有触发的action时，才执行代码，类比Pig操作）。每个RDD代表一个计算数据集的“logical plan（逻辑计划）”，直到一个明确的输出时，Spark才执行diam，比如count操作。这样的操作方式是的SPark引擎可以做一些查询优化，比如当进行pipeline（管道式操作、流水线式操作）操作的时候。例如，在上面的例子中，Spark会在读取HDFS文件的每行记录时，直接应用filter函数，来进行计数，这样操作的话，避免了存储中间的结果，如lines或errors。虽然类似的优化很有用，但是这种优化也是有限的，因为SPark引擎并不知道RDD数据中的结构（指的是任意的Java或Python类，类的结构，如字段，Spark引擎是不知道的），以及用户函数的语义（如，用户自定义函数，会引入任意的代码，而这些代码，Spark是不知道如何优化的，如果能让Spark知道这些代码，比如他执行了一个filter/where操作，那么它就可以优化，所以Spark SQL里面就是把这些代码直接用一个filter的操作进行封装，用户调用的时候直接调用filter，那么Spark引擎就会知道用户执行的是一个filter操作）。

### 2.2 Previous Relational Systems on Spark
我们第一次尝试在Spark上建立的关系型接口是Shark，这个系统可以使得Apache Hive运行在Spark之上，同时实现了一些传统RDBMS的优化，比如列式处理（Columnar Database By definition, a columnar database stores data by columns rather than by rows, which makes it suitable for analytical query processing, and thus for data warehouses.）尽管Shark使得Spark在关系型处理上拥有更高的性能，但是却有三个挑战。第一，Shark只能处理存储在Hive Catalog里面的数据，对于Spark程序中已经存在的数据并没有帮助（比如，在RDD上建立关系型查询等）。第二，从Spark调用Shark只能通过一个SQL字符串，这对于模块化的代码是非常不方便而且容易出错的。第三，Hive优化器是为MapReduce量身定做的，很难去拓展以及添加新的特性，比如机器学习中的数据类型或支持新的数据源。
### 2.3 Goals for Spark SQL
基于Shark的经验，我们想拓展关系型处理过程，使其可以处理原生的RDD，以及可以支持更多的数据源。所以，在SPark SQL中，我们设置了以下目标；
1. 提供一个关系型处理的API，同时支持原生RDD以及外部数据源；
2. 基于DBMS的技术，提供更高的效率支持；
3. 很容易可以添加新的数据源支持，包括半结构化数据或外部数据库 （可以查询元数据？ federation？）
4. 添加高级分析算法的支持，如图处理或机器学习算；

## 3 Programming Interface
在图1中可以看到，Spark SQL作为Spark之上的一个库。Spark SQL作为一个接口被暴露出来，可以使用JDBC/ODBC或一个命令行终端即可使用该接口。或者使用Spark支持的编程语言来操作DataFrame API ，进而使用Spark SQL接口。接下来将会首先介绍DataFrame API（可以整合过程编程和关系型编程代码）。但是，高级的函数也可以通过UDFs在SQL中实现，关于UDF的部分将在3.7节展开。

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure1.png?raw=true)

### 3.1 DataFrame API
在 Spark SQL中使用的主要的封装API就是DataFrame（一个分布式的拥有同样的模式（schema，可以理解为列名和列类型，就像数据库中的元数据信息一样）的行数据集合）。一个 DataFrame和传统关系型数据库中的表等价，而且还可以象原生的分布式数据集（指RDD）一样被操作（说的就是API是近似的）。但是和RDD不一样的是，DataFrame会跟踪模式（Schema）的处理过程，同时支持很多能被优化的关系型操作。

 DataFrame可以通过外部表（或外部数据源）创建，或从已存在的RDD（一个Java或Python的类的RDD）中创建（参见3.5节）。一旦被创建好，就可以执行很多关系型操作，如wehere，groupBy等，这些操作接受表达式（这种表达式是一种DSL，领域特定语言？）作为参数，这种操作和R以及Python的data frame类似。DataFrame也可以被看做由Row类型的RDD组成，这样就可以在其上执行类似过程型处理API了，如map操作。
 
 最后，和传统data frame的API不同的是，Spark的DataFrame是“懒”的（lazy），指的是每个DataFrame 代表一个计算某个数据集的逻辑计划，但是并没有任何实际计算的发生直到明确调用了“输出操作”，如save（保存）操作。这个特性使得Spark可以对DataFrame上的操作进行更多的优化。
 
 为了说明，下面的Scala代码定义了一个从Hive中读取的DataFrame，并且通过这个DataFrame计算得到另一个DataFrame，最后打印结果。
 ```
 ctx = new HiveContext()
 users = ctx.table("users")
 young = users.where(users("age") < 21)
 println(young.count())
 ```
 在代码中，变量users、young是一个DataFrame。代码片段users("age") < 21 就是一个 expression（表达式，DSL），这种表达式被定义为一个抽象语法树，而不是像传统Spark API中的一个Scala函数。（根据抽象语法树（AST）Spark就知道代码实际执行的是啥，而如果是一个Scala函数，Spark是没办法知道的）。总的来说，每个DataFrame就是一个逻辑计划。当用户调用count函数时（一个输出操作），这是Spark就会进行优化，比如如果原始数据是一个列式存储系统，那么可以只读取age列，或只使用数据的索引来对符合的行进行计数，以此来提升效率。
 
 接下来，会对DataFrame进行详细介绍。
 
 ### 3.2 Data Model
 Spark SQL为DataFrame选用一个基于Hive的嵌套的数据模型。其支持所有主流的SQL数据类型，如boolean，interger，double，decimal，string，data，timestamp以及复杂数据类型（非原子类型）：structs（结构类型），arrays（数组），maps（键值对），unions（联合类型）。复杂类型也可以进行嵌套以实现更加有用的类型。和很多传统DBMSes（数据库管理系统）不同，Spark SQL为复杂数据类型和API提供了一流的支持。同时，Spark SQL也支持用户自定义类型（在4.4.2节将会介绍）。
 
 使用这种类型的框架，我们可以对很多数据源或不同格式数据进行非常精确的数据定义（model data，model这里应该是动词），这些数据源或格式包括HIve，传统数据库，JSON，原生Java、Scala、Python类。
 
 ### 3.3 DataFrame Operations
 用户可以在DataFrame上使用DSL（表达式）进行一系列关系型操作，就像R中的data frames以及Python中的Pandas一样。DataFrame支持常见的关系型操作，包括projection（是一种操作，直译为投影，可以理解为一种数据展现，比如这种操作的一个select，其实就是查询，查询就会有结果，而这个结果就是原始数据的“投影”，可以这样理解），filter（过滤操作，如where操作），join和aggregations（聚合操作，如groupBy）。这些操作都使用表达式（expression），由于这些表达式都是由有限的DSL组成的，所以Spark可以知道每个表达式的结构。例如，下面的代码计算每个department中feamal employee的个数。
 ```
 employees
    .join(dept, emplyees("deptId") === dept("id"))
    .where(employees("gender") === "female")
    .groupBy(dept("id"), dept("name"))
    .agg(count("name"))
 ```
 在这段代码中，employees是一个DataFrame，employees("deptId")是一个代表deptId列的表达式。基于表达式（Expression）可以进行很多操作，然后返回仍然是表达式，包含常见的比较操作（如 === 代表相等测试,> 代表大于）和算数操作（如+，-）。表达式也支持聚合操作，如count("name")。所有这些操作建立了一个表达式的抽象语法树（AST），而AST接下来就会被Catalyst进行优化。这就和传统Spark API使用任意的Java，Scala，Python代码的函数进行传递，这样就会导致这些函数里面的具体操作对于Spark执行引擎来说是不透明的，也就说不上什么优化了。如果想查看上面代码中具体API，可以查看Spark官网。
 
 除了关系型的DSL之外，DataFrame也可以被注册成为一个系统中的临时表，然后就可以使用SQL来进行查询。下面的代码就是一个示例：
 ```
 users.where(users("age") < 21)
        .registerTempTable("young")
ctx.sql("select count(*) , avg("age") from young")
 ```
  这种SQL注册表的方式，在某些场合如聚合操作中可以很方便的进行操作，且表意清晰，同时可以使得程序通过JDBC/ODBC来访问数据。通过注册临时表的方式，仍然是没有序列化的视图，所以在SQL以及DataFrame 表达式中仍然有优化的空间。但是，DataFrame也可以被实质化（序列化? 存储？），在3.6节讨论。
  
  ### 3.4 DataFrames versus Relational Query Languages
  虽然，表面上来看，DataFrame和如SQL或Pig一样提供关系型查询语言的操作，但是我们发现由于Spark SQL可以整合入多种编程语言中，所以对于用户来说，Spark SQL会非常易于使用。比如，用户可以把代码分解成Scala，Java，或Python的函数，并把DataFrame传到这些函数中，以此来建立一个逻辑计划，同时仍然可以在整个逻辑计划中享有Spark的优化（当执行输出操作时就会进行优化）。类似的，开发者可以使用控制结构，像 if 语句 或 循环语句 来完成任务。 一个用户提到DataFrame的API是非常简明的，它的声明式就和SQL一样，除了可以对中间结果进行命名，体现出structure computations 和 debug 中间结果是容易的一件事情。
  （这一段说的是DataFrame和传统关系型查询语言SQL的对比，DataFrame可以对中间结果命名，这是第二个优点；第一个是？）
  
为了简化在DataFrame中的编程，我们会在API中提前分析逻辑计划（比如识别expression（表达式）中的列名是否在给定的表中，或给定的列数据类型是否是合适的（可以理解为是否和数据库中是匹配的）），但是其执行仍然是lasy的。所以，Spark SQL 会在用户输入一行非法的代码的时候就报错，而不是等到执行的时候。这种处理对于用户来说，同样是一个减负的操作（好过处理一个大的SQL）。
### 3.5 Querying Native Datasets
真实业务流程经常从很多异构的数据源中抽取数据，接着使用很多不同的算法库来对数据进行分析。为了能够和过程型Spark代码互通，SPark SQL允许用户可以直接从RDD来构造DataFrame。Spark SQL 可以通过反射自动得到元数据（Schema）信息（也就是列信息，如列名，列类型等）。在Scala或Java中，数据类型信息通过JavaBeans或Scala的Case class获取。在Python中，Spark SQL 对数据集进行抽样，然后动态的去获取元数据信息（动态获取，就是先看能否转换为double，然后看能否转换为int，最后才是string，基本就是这种思想）。

举例来说，在下面的Scala代码中定义了一个DataFrame（从RDD[User]转换而来）。Spark SQL 自动的识别了列名（如“name”和“age”）以及其对应的数据类型（string，int）。
```
case class User(name:String, age:Int)
// create an RDD of User objects
usersRDD = spark.parallelize(List(User("Alice",22),User("Bob",19)))
// view the RDD as a  DataFrame
usersDF = usersRDD.toDF
```
 在底层实现上，Spark SQL会创建一个指向RDD的逻辑数据扫描操作。这个操作会被编译成一个可以接触原始对象的字段（原始对象就是指的User类）的物理操作。需要注意的是，这种操作和传统的ORM（类关系映射）是非常不一样的。ORM系统一般在把整个类转换成不同的格式的时候会引起很大的转换消耗。但是，Spark SQL却可以直接就地操作字段，所以可以根据每个查询需要的字段来进行提取。
 
 查询本地数据集的特性（直接访问类的字段）使得用户可以在现有的Spark代码中执行关系型操作的优化（说白了，就是在原RDD的代码中引入Spark SQL的优化机制）。同时，如果用户想把RDD和一个外部的结构化数据源进行合并，那也会非常简单。例如，可以把users RDD（上一个代码）和Hive中的一个表合并：
 ```
 views = cxt.table("pageviews")
 usersDF.join(views,usersDF("name") === views("user"))
 ```
 
 ### 3.6 In-Memory Caching
 和Shark类似，SPark SQL也可以使用列式存储在内存中缓存热数据（hot data，经常使用的数据一般称为热数据）。和Spark原生的缓存机制（直接把数据作为JVM类存储）不同的是，列式系统存储缓存可以减少一个量级的内存占用空间，因为Spark SQL应用柱状压缩方案（columnar compression schemes），比如字典编码及行程编码（run-length encoding：行程编码（Run Length Encoding，RLE), 又称游程编码、行程长度编码、变动长度编码 等，是一种统计编码。主要技术是检测重复的比特或字符序列，并用它们的出现次数取而代之。）缓存技术对于迭代查询，特别是对于机器学习中的迭代算法非常有用。可以在DataFrame中调用cache()进行缓存。
 
 ### 3.7 User-Defined Functions
 用户自定义函数（UDFs）是对数据库系统的一个很重要的拓展。比如，MySQL中使用UDFs来提供对JSON数据的支持。一个更高阶的例子是MADlib的UDFs的使用，它可以在Postgres 或其他数据库中实现学习算法。但是，数据库系统一般需要使用不同的编程环境（比如Postgres里面使用Java来开发UDF，而本身使用的是Postgres的环境，也就是不能直接使用Postgres SQL的环境来实现UDFs）来实现这些UDFs。Spark SQL中的DataFrame API却可以不需要额外的编程环境，就可以直接实现UDFs，同时还不用复杂的打包、注册操作过程。这也是该API的一个重要的特性。
 
 在Spark SQL中，UDFs可以通过Scala，Java或Python函数来注册生成（which may use the full Spark API internally：暂时不确定翻译）。例如，给定一个机器学习模型中的model的变量，可以把其预测函数重新注册生成为一个UDF：
 ```
 val model: LogisticRegressionModel = ...
 ctx.udf.register("predict",(x:Float,y:Float) => model.predict(Vector(x,y)))
 ctx.sql("SELECT predict(age,weight) FROM users")
 ```
 UDFs被注册后，就可以通过JDBC/ODBC来给其他商业智能工具调用。UDFs除了可以处理标量数据外，还可以使用Spark API来处理整个表（使用分布式的Spark API，其实就是Spark Core API），也就是为SQL用户提供了很多高级的分析函数。最后，UDF函数定义和查询引擎都是使用通用的语言（如Scala或Python）来编写的，所以用户可以使用标准工具来进行debug（调试，如使用IntelliJ IDEA或Eclipse、PyCharm工具来调试等）。
 
 上面的例子说明了一个在流程化处理中的通用例子。如，需要用到关系运算或高级分析函数处理的场景，那么在SQL中来实现是很复杂的。但是，DataFrame API可以无缝的衔接这两种处理手段（这里指的是这两个分析手段，还是说多个分析函数？）。
 
 ## 4. Catalyst Optimizer
 为了实现Spark SQL，我们基于函数式编程语言Scala设计了一个新的增强优化器，Catalyst。Catalyst的增强设计有两个目的。第一个，我们希望能在Spark SQL中很容易的添加新的优化技术及特性，特别是解决多种大数据问题（比如，半结构化数据和高级分析主题）。第二，我们想让外部开发者帮我们扩展优化器（
 for example, by adding data source specific rules that can push filtering or aggregation into external storage systems, or support for new data types.）Catalyst 支持基于规则或基于成本（运行耗时等）的优化。
 
 虽然可拓展的优化器在之前就被引入，但是需要复杂的特定领域语言（domain specific language）来表达规则，同时，需要一个“优化器编译器”来把规则转换为可执行代码。这造成了很大的学习曲线和维护负担（也就是别人修改或维护比较难）。相对的，Catalyst使用Scala标准的特性来开发，比如pattern-matching（模式匹配）。这样，开发者仍然可以使用Scala的全部特性来构建规则。函数式语言被用来构建编译器，所以我们发现Scala很适合被用来构建Catalyst。尽管如此，在我们的认知中，Catalyst仍然是一个质量很高使用函数式编程语言实现的查询优化器。
 
 Catalyst的核心包含一个用来表达规则的通用库（and applying rules to manipulate them，them指啥？应用规则来操作them，them指啥？）。在这个框架上，我们创建了很多对应于关系型查询处理的库（如，表达式，逻辑计划等），以及一些可以处理查询执行（query execution）的不同阶段的规则，查询执行的阶段有：分析，逻辑优化，物理计划，代码生成（会编译部分查询，并生成Java二进制代码）。关于代码生成，我们使用了Scala另外的一个特性，quasiquotes（?）它可以使运行时代码生成很简单（通过组合表达式，实际指的是可以直接用字符串来代替代码）。最后，Catalyst提供多个公共的拓展接口，包括外部数据源和用户自定义类型。
 
 ![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure2.png?raw=true)
 ### 4.1 Trees
 在Catalyst中最重要的数据类型就是由一系列节点构成的树结构。每个节点有一个节点类型以及零个或多个子节点。如果要定义新节点类型，那么可以继承Scala中的TreeNode class。这些类是不可变的，同时可以使用函数式转换（transformations）来操作。下面将会介绍这些函数式转换（transformations）。
 
 简单来说，假设我们有下面三个节点类，并用这三个节点类来构建一个表达式（expression）：
-  Literal(value:Int): 常亮类；
-  Attribute(name:String): 输入row的一个属性，如“x”；
-  Add(left:TreeNode,right: TreeNode) : 两个表达式的和；

使用这些类就可以构建树：比如，构建表达式x+(1+2)的树，可以使用下面的Scala代码来构建（参考图2）：
```
Add(Attribute(x), Add(Literal(1)), Literal(2))
```
### 4.2 Rules
规则就是一个函数，可以把一个树转换生成另外一个树，所以可以使用规则来操作树。While一个规则可以在输入的树上运行任意的代码（这里的树指的是一个Scala的类），最常使用的方式是使用一系列的模式匹配（pattern matching）函数来找到以及替换具有特定结构的子树。

模式识别（pattern matching）是很多函数式编程语言都具有的一个特性，可以从可能的嵌套的代数数据类型（algebraic data type）结构中找到匹配的值。在Catalyst中，trees（树结构）提供transform方法，可以应用模式识别函数来递归的遍历树中的所有节点，这样就可以针对每个节点来匹配与之对应的结果或模式。例如，可以使用如下的方式实现常量之间的加法：
```
tree.transform{
    case Add(Literal(c1), Literal(c2) => Literal(c1+c2))
}
```
把上面的函数应用到 x+(1+2)就会生成一个新的树x+3.这里的‘case’关键字是Scala标准的模式识别语法格式，‘case’可以匹配object的类型以及给定的名称来提取值（比如这里的c1，c2）。

传给transform的模式识别表达式是一个偏函数（partial function，具体定义：），也就是表达式只需要匹配所有可能的输入树的一部分（或理解为一个）即可。Catalyst会测试一个给定的规则可以应用到树种的哪个部分，同时在不匹配的时候自动的跳过或者遍历其子树。这个特性意味着规则只会针对匹配树应用优化，而对不匹配的则不用应用。所以，这些规则不需要作为新的操作符添加到系统中。（说的意思就是，这些规则是类似一个plugin，可以随插随用，而不需要改动系统的代码）。
 
 在同一个transform调用中规则可以同时匹配多个模式，所以实现多个转换匹配将会非常简单，如下：
 ```
 tree.transform{
     case Add(Literal(c1), Literal(c2)) => Literal(c1 + c2)
     case Add(left, Literal(0)) => left
     case Add(Literal(0), right) => right
 }
 ```
 事实上，可能需要多次应用规则才能转换一个树。Catalyst 把规则进行分组，称为批操作（batches），同时针对每个批处理递归执行，直到到达一个点，这个点就是当再次应用规则的时候，树不会改变。操作树到一个点指的是每个规则需要是简单的或独立的，同时最终这些处理后的规则加起来可以达到一个全局的效果。在上面的例子中，重复应用就可以得到一个更大的树，如(x+0) +(3 + 3).另外一个例子，第一个批处理可能会分析表达式，同时为每个属性匹配类型，而第二个批处理可能使用这些类型进行常量折叠（就是先合并常量项）。执行每个批处理后，开发者页可以进行新生成的树上执行完整性检查（例如，检查是否所有的属性都匹配到了类型。这些完整性检查也可以通过递归的匹配来达到）。
 
 最后，规则的条件和规则的实现可以包含任意的Scala代码。这个特性使得Catalyst比领域特定语言（domain specific language）在优化器上更具优势，同时保持了简单规则的实现简洁性。
 
 根据我们自己的经验，对不变的树应用函数式转换操作可以使得整个优化器很容易进行推导及调试。They（指谁？或者某个技术？）也在优化器中实现了并行处理，尽管我们还没有利用这个特性。
 
 ### 4.3 Using Catalyst in Spark SQL
 我们使用四个步骤来实现Catalyst一般的树转换框架（如图3所示）：
1. 通过分析逻辑计划来解决引用问题；
2. 优化逻辑计划；
3. 物理计划；
4. 编译部分查询代码到Java二进制（代码生成）；

在物理计划阶段，Catalyst可能生成多个计划，同时会根据成本来比较这些计划并选择某个计划。其他三个阶段都是基于规则的。每个阶段使用不同的树节点类型，Catalyst内含表达式、数据类型、逻辑、物理操作符相关的节点库。接下来详细描述着四个阶段。
![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure3.png?raw=true)
#### 4.3.1 Analysis
Spark SQL最开始是一个待计算的关系表达式（只有一个relation，可以附带一个表达式，理解起来更容易些），这个表达式要么是从一个SQL解析器中得到的抽象语法树（AST，abstract syntax tree），要么是通过DataFrame API得到的。不管是怎样得到的，待计算的关系表达式都可能包含仍未解析的属性引用或其他关系表达式。例如，在如下的SQL中 ： SELECT col FROM sales，如果我们不查看表sales的话，那么对于col的类型我们是不知道，甚至我们都不知道col列名是否是一个合法的列名。一个未被解析的属性指的是还不知道该属性的数据类型或者该属性不能匹配输入表的字段（或者别名）。Spark SQL中使用Catalyst规则以及Catalog引用（可以理解为元数据信息，有所有表的相关信息）来跟踪所有数据源中的表，以此来解析其出现的属性信息。最开始，会构建一个“未解析的逻辑计划（unresolved logical plan）”树，这个树包含未绑定的属性及数据类型，接着会应用规则来解析，具体如下：
- 在catalog中根据名字查找关系（此处的关系可以理解为表）；
- 映射列，如列名 col，to the input provided given operator’s children；
- 确定哪些属性是一样的，同时给他们一个唯一的ID（后面会针对表达式进行优化，如col=col，可以防止同样的属性被解析多次，降低效率？）
- 通过表达式推断类型（propagating and coercing types through expressions），例如：针对表达式1 + col，如果想知道这个表达式的数据类型，那么就需要先知道col的类型，然后尽可能的把该表达式的数据类型转换成合适的类型。

总的来说，analyzer的实现代码大约有1000行。

#### 4.3.2 Logical Optimization
在逻辑优化阶段，会对逻辑计划应用标准的基于规则的优化，包括常量合并（constant folding）、predicate pushdown（谓词下推？简而言之，就是在不影响结果的情况下，尽量将过滤条件提前执行。）、projection pruning（映射修剪？）、null propagating（空传播？它将空的源值直接转换为空的目标值），布尔表达式简化及其他规则。一般来说，在很多的情况下添加规则都非常简单。比如，当我们在Spark SQL中添加了一个固定精度的DECIMAL类型，那么如果要对DECIMAL进行小精度的聚合操作，如SUM或AVG操作，那么使用12行代码就可以写一个规则来实现这样的需求：先把其转换为一个unscaled 64-bit LONGs，接着对其进行聚合操作，得到结果后，再次转换即可。一个简单的规则实现如下（只实现了SUM操作）：
```
object DecimalAggregates extends Rule[LogicalPlan] { 
    /** Maximum number of decimal digits in a Long */ 
    val MAX_LONG_DIGITS = 18
    def apply(plan: LogicalPlan): LogicalPlan = { 
        plan transformAllExpressions {
            case Sum(e @ DecimalType.Expression(prec, scale)) 
                if prec + 10 <= MAX_LONG_DIGITS =>
            MakeDecimal(Sum(LongValue(e)), prec + 10, scale) }
}
```

另外一个例子，LIKE表达式可以通过12行类似的规则来进行优化，可以简单的使用String.startsWith或String.contains来实现。在规则中可以使用任意Scala代码使得类似的优化比使用模式识别能很容易表达出来。

总的来说，逻辑优化规则代码有近800行代码。

#### 4.3.3 Physical Planning
在物理计划阶段，Spark SQL根据一个逻辑计划，使用Spark执行引擎可识别的物理操作符（其实就是Spark Core API？）生成一个或多个物理计划。接着，使用成本代价模型来选择一个计划。目前，根据代价模型的优化器只用在了Join操作上：针对比较小的数据集（for relations that are known to be small），Spark SQL使用broadcast join（Spark中的一个 peer-to-peer broadcast facility ？ 是啥？）这个框架支持广泛的基于代价模型的优化，但是，代价需要对整个树应用规则来进行递归评估，所以将来将会实现更多的基于代价模型的优化算法。

物理执行器也会进行基于规则的物理优化，比如在一个map函数中直接应用pipelining projections（管道投影？）或者过滤。另外，针对支持predicate或者projection pushdown（谓词下推）的数据源可以从逻辑计划push operations（？） 。在 4.4.1中会介绍这些数据源相关的API。
 
总的来说，物理计划规则的代码有将近500行。

#### 4.3.4 Code Generation
最后一个查询优化阶段包含生成在每个机器上运行的Java二进制代码。因为Spark SQL经常操作内存中的数据集（这个操作是CPU受限的），所以，我们想支持代码生成，以此来加速执行。尽管如此，代码生成引擎一般构建都比较复杂，amounting essentially to a compiler。Catalyst基于Scala语言的一个特殊的特性，“quasiquotes”（把字符串替换为代码的特性），使得代码生成更加简单。Quasiquotes允许在Scala中使用代码构建抽象语法树（AST），构建的抽象语法树会被传给Scala编译器，进而在运行时生成二进制代码。我们使用Catalyst来转换一个SQL表达式代表的树到AST，以此就可以使用Scala代码来进行评估该表达式（可以理解为执行SQL表达式），进而编译和执行生成的代码。

一个简单的例子，在4.2节中引入的Add操作，其中的属性和字面量树节点，可以使用这些简单的构造这样的一个表达式：(x+y)+1. 如果没有代码生成，这样的表达式就会针对数据的每行进行操作，也就是从Add，属性，常量构成的树的根节点开始，往下遍历，这就会造成大量的分支和virtual function（虚拟函数？）的调用，从而降低执行的效率。如果使用代码生成，那么就可以编写一个函数来把某个固定的表达式树转换成一个Scala的AST，如下：
```
def compile(node: Node): AST = node match { 
    case Literal(value) => q"$value"
    case Attribute(name) => q"row.get($name)"   
    case Add(left, right) =>
        q"${compile(left)} + ${compile(right)}"
}
```
以q开头的字符串就是quasiquotes，意味着尽管这些看起来像字符串，但是他们会被Scala编译器在编译的时候转换成表示AST树的代码。Quasiquotes可以使用“$”符号连接变量或其他AST树。例如，Literal(1)在Scala AST树种直接转换为1，而Attribute("x")可以转换为row.get("x")。所以，类似Add(Literal(1), Attribute("x"))的AST树就会生成1+row.get("x")的Scala表达式。

Quasiquotes在编译的时候回进行类型检查，以确保AST或字符串能被恰当的替换（这个功能比字符串的拼接更加的实用，同时，它可以直接生成Scala的AST而不是在执行时还需要Scala转换器（parser））。此外，它们是高度可组合的，因为每个节点代码生成规则不需要知道其子节点生成的树是怎么样子的。最后，生成的代码可以被Scala编译器进行表达式级别的优化，以防Catalyst错过。图4对比了使用Quasiquotes生成的代码效率和手动优化的代码效率。

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure4.png?raw=true)

我们发现使用quasiquotes来进行代码生成整个逻辑很清晰，所以就算是新的参与者也可以很容易的针对新表达式的类型添加规则。Quasiquotes也可以在原生Java类型上工作的很好，当访问Java类中的字段时，可以生成一个直接字段的访问，而不是拷贝类到一个Spark SQL中的Row，然后使用Row的方法来访问某个字段（这样当然效率高）。最后，因为我们编译的Scala代码可以直接调用我们的表达式解释器，所以虽然整合表达式的代码生成式评估（code-generated evaluation）和直译式评估（interpreted evaluation）不复杂，但是我们还没有对这块进行代码生成。

最后，Catalyst的代码生成器一共有700行左右代码。

### 4.4 Extension Points
Catalyst针对可组合的规则设计使得用户或第三方可以很容易的进行拓展。开发者可以针对执行阶段的查询优化器的多个阶段添加多批次规则，只要他们遵守一定的规则（如保证所有的变量都得到解析等）。但是，如果想在不需要理解Catalyst规则的前提下，使得添加一些类型的拓展变得更加简单，那么就需要一些其他的模块，为此，我们也添加了两个轻量级的公共拓展模块：数据源（data sources）和用户自定义类型（user-defined types）。这两个模块同样需要依赖核心引擎来与优化器的其他部分进行交互。

#### 4.4.1 Data Sources
开发者可以使用Spark SQL的多种API来定义新的数据源，通过这些API定义的数据源可能会触发不同级别的优化。所有数据源都需要实现createRelation函数，此函数接受一个键值对的set参数，返回一个代表此关系的BaseRelation的类（if one can be successfully loaded，指的应该是自定义的数据源的类能被成功加载）。每个BaseRelation包含一个schema（元数据）和一个可选的使用bytes呈现的估计大小（批注：非结构化数据源可以使用一个用户期望的schema作为参数，例如，一个CSV文件数据源可以让用户设置列名和列类型）。例如，一个代表MySQL的数据源可能需要一个表名作为参数，同时会向MySQL请求该表的估计大小（table size）。

为了使得Spark SQL可以读取数据，BaseRelation可以实现多个接口中的一个，let them expose varying  degrees of sophistication（可以使得他们把不同级别的复杂性暴露出来？sophistication翻译为复杂性还是成熟度？）其中最简单的TableScan，需要the relation返回一个RDD[Row]，这个RDD包含数据表中的所有记录。更高级的PrunedScan接收一个列名数组，并且返回只包含给定列的Row数组。第三个接口，PrunedFilteredScan接收设置的列名和一个Filter类型的数组参数（Catalyst表达式的语法），可以进行谓词下推（predicate pushdown，当前过滤器支持相等判断、和常量对比、IN语法，each on one attribute）。过滤器需要是具有advisory（可以理解为强健性），例如，数据源需要返回能通过每个过滤器的数据，同时需要针对不能够评估的数据也需要返回false（也就是说针对不能评估的数据，不是报错，而是直接过滤掉这些数据）。最后，CatalystScan接口is given a complete sequence of Catalyst 表达式树in predicate pushdown，尽管他们也是advisory的。
 
这些接口使得数据源可以实现不同级别的优化，同时仍可以使得开发者可以很简单的添加计划任何简单数据源的类型。目前已经使用这些接口实现了以下数据源：
- CSV文件数据源，简单的扫描整个文件，但是也允许用户设置一个元数据（schema）；
- Avro数据源，一个自描述二进制格式的嵌套式数据源；
- Parquet数据源，一个列式文件格式（像filters一样支持列裁剪（column pruning，在读数据的时候，只关心感兴趣的列，而忽略其他列））；
- JDBC数据源，可以并行的扫描RDBMS的表区间数据，同时可以添加过滤器来最小化数据传输。

如果想要使用这些数据源，程序员需要在SQL表达式中指定他们的包名，同时可以把键值对传入从而进行对配置项进行修改。例如，Avro数据源可以接受一个路径参数，如下：
```
CREATE TEMPORARY TABLE messages 
USING com.databricks.spark.avro 
OPTIONS (path "messages.avro")
```
所有的数据源可以附加网络本地化信息（network locality information），例如，可以附加如下信息：数据的每个分区从哪个机器读取更加高效。这个信息的附加，是通过返回的RDD类添加的，因为RDD中有内建的数据本地化API。

最后，把数据写入已存在或新表的接口也是存在的。这些接口更加简单，因为Spark SQL提供了一个RDD的Row类用来作为写入数据的类型。

#### 4.4.2 User-Defined Types（UDTs）
在Spark SQL中，我们希望添加一个能允许高级分析的特性就是用户自定义类型（user-defined types）。例如，机器学习相关应用可能需要一个向量类型，图算法可能需要一个类型来表示一个图（这在关系型表中是可能的）。虽然，添加新的类型非常具有挑战性，但是，数据类型存在于执行引擎的各个方面，所以我们还是添加了这个特性。例如，在Spark SQL中，内置的数据类型使用列式压缩格式存储，方便在内存中进行缓存（见Section 3.6节）。同时，在前一节中提到的数据源API，我们需要把所有可能的数据类型暴露给新数据源的创建者。

在Catalyst中，我们通过映射用户自定义类型到由Catalyst内置的类型的组合及结构化处理的类型来解决这个问题（在3.2节已作说明。为了注册一个Scala的类型作为UDT，用户需要提供一个类到Catalyst Row的内置类型的映射关系以及相反的映射关系。在用户代码中就可以使用Scala类型，在Spark SQL的查询语句中，这些Scala类型就会在底层被转换为内置的类型。同样的，他们也可以直接注册UDFs（见Section3.7），来直接操作他们的类型。

举一个小例子，假设我们想注册一个二维的点（x,y）作为一个UDT。我们可以使用两个DOUBLE类型来代表这样的向量。可以使用下面的代码来注册一个UDT：
```
class PointUDT extends UserDefinedType[Point] {
    def dataType = StructType(Seq( // Our native structure
        StructField("x", DoubleType),
        StructField("y", DoubleType) 
    ))
    def serialize(p: Point) = Row(p.x, p.y) 
    def deserialize(r: Row) =
        Point(r.getDouble(0), r.getDouble(1)) 
}
```
使用上述代码，就可以把Points类型的数据转换为本地类型，也就是在Spark SQL中可以把这样的数据转换为DataFrame类型，同时，可以使用在Points类上定义的UDFs来进行操作。另外，Spark SQL会在对Ponits类型的数据进行缓存时使用列式存储（把x，y压缩并存储到不同的列中），同时Points类型数据可以写入到Spark SQL中支持的数据源中（在数据源中使用DOUBLE对来存储）。我们将会在Section5.2中说明如何在Spark机器学习库中使用这一特性。

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure5.png?raw=true)

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure6.png?raw=true)

## 5 Advanced Analytics Features
在本节中，主要描述了三个新添加到Spark SQL中的特性，这三个特性是针对“大数据”的环境中挑战所提出的。第一，在常见的大数据环境中，数据经常是非结构化或半结构化的。虽然解析这样的数据从程序上来说是可行的，但是常常其处理代码会包含很多冗长的模板式的代码。为了使用户能立马查询数据，Spark SQL 内置了一个专门为JSON和其他半结构化数据处理的元数据推算算法。第二，大批量处理经常会进行聚合、连接操作，特别是机器学习处理。我们把Spark SQL嵌入到一个更高级别的API中，作为Spark机器学习库处理的手段。最后，数据管道处理进程把不同的存储系统的数据进行整合。基于4.4.1节提出的数据源API，Spark SQL支持查询联邦（query federation），允许单个程序高效的差不同的数据源。这些特性都基于Catalyst框架。

### 5.1 Schema Inference for Semistructured Data
在大规模环境中半结构化数据是非常常见的数据源，因为这样的数据很容易生成，同时，随着时间的推荐，也很容易添加列（如果是固定列的表，那么添加列就需要修改元数据，比较麻烦）。在Spark用户中，很大部分都使用JSON作为输入数据。不幸的是，在Spark和MapReduce这样类似的框架中，JSON是一个非常难以操作的类型。比如很多用户会把数据进行恢复，而使用类似ORM的映射库（如使用Jackson）来把JSON结构映射成Java类，或一些用户会使用一些底层API直接对每行数据进行转换。

在Spark SQL中，我们添加的JSON数据源，可以直接从部分数据记录中获得元数据。例如，给定一个如图5所示的JSON数据，通过推导可以得到如图6所示的元数据。用户可以直接把JSON文件注册成一个表，从而可以直接使用SQL语法来访问其中的字段，如：
```
SELECT loc.lat, loc.long FROM tweets
WHERE text LIKE ’%Spark%’ AND tags IS NOT NULL
```
元数据推理算法需要读取数据一次，当然如果设置参数也可以只读取部分抽样数据。这个算法基于之前的XMLS和数据库之间的转换的工作基础上，但是更加简单，因为只需要得到一个静态的树结构，而不是得到一个在任意一个元素上进行递归嵌套，从而允许有任意的深度。

特别的，算法尝试去获得一个STRUCT类型的树结构，每个STRUCT类型可能包含原子类型、数组或其他STRUCTs类型。针对一个唯一路径中的JSON类根节点的每个字段（如tweet.loc.latitude）,算法会为其找到其最匹配的Spark SQL中的类型（最匹配也就是找到字段对应的值，然后一个个匹配，看其是否是数值、字符串等）。例如，如果某个字段的值都是整数，并且能够刚好放入32bits，那么字段类型就是INT；如果不能放入32bits，那么就是LONG（64-bit）或DECIMAL（任意精度）类型；如果有小数值，那么就是FLOAT类型。针对会出现多个类型值的字段，Spark SQL使用STRING来作为其类型，并且保持原始JSON中出现的字符串。针对包含数组的类型，那么会使用上述类似的思路来找到数组中每个元素的类型。我们通过reduce函数来实现这个算法，这个算法从每个记录的schemata开始推断每个记录的字段类型，然后使用“most specific supertype”函数整合这些字段类型（most specific supertype函数指的是最多匹配的类型）。这样使得算法只需要一次读取以及高效传输，因为在每个节点可以进行本地聚合操作。

举个小例子，在表5和表6中，算法针对loc.lat和loc.long字段都进行了泛化。在其中的一个记录中，每个字段都是interger类型，但是在另一个记录中，是一个floating的类型，所以最终返回FLOAT类型。注意到tags字段，算法推断出其类型为字符串的数组类型，同时不能为null。

实际上，我们发现这个算法在现实生活中的JSON数据集中应用很好。例如，它能正确的识别tweets的JSON（Twitter's firehose数据集）并得到一个可用的元数据，其包含大约100个不重复字段和多级嵌套的字段。同时，多个Databricks的用户都已经成功应用该算法到其内部JSON数据集中。

在Spark SQL中，我们同样适用该算法来得到Python类RDD的元数据（见Section 3），同时因为Python的数据类型是非静态类型，所以一个RDD可以包含不同的数据类型。我们计划在后面会添加CSV和XML文件的元数据推导。开发者能很容易把数据集转换转换为表，转换为表后可以直接进行查询或与其他数据进行连接。他们（开发者）认为这样的操作对其生产环境具有很大价值。

### 5.2 Integration with Spark's Machine Learning Library
作为Spark SQL在其他Spark模块中的应用的例子，如Spark MLlib（机器学习算法库），在其中引入了一个使用DataFrame的高阶API[26]。这个新API是基于机器学习的pipelines（管道、流水线）的理念，这个理念是其他一些高阶ML库（例如SciKit-Learn[33]）的一种抽象。流水线作业指的是在数据上的一系列转换操作，例如特征提取（feature extraction）、归一化（normalization）、降维（dimensionality reduction）和建模（model training），这些过程前一个的输出对应后一个的输入，从而构成一个流水线作业。一般来说，流水线作业是一个非常有用的抽象，因为ML的工作流一般由很多个步骤组成。把这些步骤表示成可组合的项使得改变流水线中的某个环节或对整个流水线作业进行参数寻优都会变得非常容易。

为了进行pipeline stages之间的数据交换，MLlib的开发者需要一种比较紧凑（是因为数据可能很大，所以需要比较紧凑）并且仍保持灵活的格式，同时允许每行记录可以存储多种类型的字段数据。例如，一个用户拿到一个记录集，包含文本列和数值列，接着，可以针对文本列执行一个特征化算法（例如TF-IDF），从而变成一个向量，对其他的数值字段执行归一化操作，对整个数据集执行降维操作等等。新API使用DataFrame来表示这个数据集，在DataFrame中，每个列代表数据中的一个特征。所有可以在Pipeline中调用的算法都可以接收输入列名和输出列名的参数，以及任意输入列名的子集，从而产生新的数据集。这使得开发者可以很容易的在保留原始数据的情况下，构建复杂的pipeline。为了说明这个API，在图7中简单展示了一个简单的pipeline，以及创建的DataFrame的schema。

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure7.png?raw=true)

为了在MLlib中使用Spark SQL，只需要创建一个用户友好的vector数据类型。这个向量UDT可以存储稀疏向量或密集向量，使用四个基本数据类型字段即可表示：一个boolean表示类型（是dense或sparse），vector的大小，下标数组（作为sparse的索引），一个代表值的double数组（只针对spark的非零值才有显示，针对dense数据，则全部显示）。使用DataFrame除了可以跟踪和操作列外，我们还发现了一个其他的原因：通用性（也就是使用DataFrame后，这些API可以在多种Spark支持的预研中通用，例如支持Scala、python 、R等）。而在这之前，在MLlib中的每个算法都有自己的一套数据结构，例如分类中使用labeled point类，而使用推荐算法则需要使用rating类，同时，这些类需要在不同的语言中都实现一遍（例如，需要把相同的代码在Scala、Python、R中都实现一遍）。使用DataFrame可以在所有语言、所有算法中达到很高的通用性（这种通用性抽象出来就是数据的转换，而数据的转换在Spark SQL中天然的就存在的）。而这个特性在Spark添加新的语言支持的时候就显得尤为重要。

最后，在MLlib中使用DataFrame来进行存储后，在SQL中使用这些算法也会非常简单。我们可以简单的定义类MADlib的UDF（用户自定义函数，user defined function），就像在3.7节中描述的一样，最终在内部会在表上调用算法。我们当前也在研究在SQL中使用pipeline的方法。

### 5.3 Query Federation to External Databases
数据管道（data pipeline）经常需要整合异构的数据源。 例如，一个推荐的流水线任务可能需要整合一个用户信息库中的访问日志数据和用户的社交流数据。鉴于这些数据源经常在不同的机器或物理隔离的位置上，直接来查询这些数据将会导致非常低效。Spark SQL中使用Catalyst来对数据源进行 predicate down （一种优化机制）优化。

例如，下面的代码中从一个JDBC数据源和一个JSON数据源中读取数据，得到两个表，并把两个表进行join操作，以此来从访问日志中得到最近注册的用户。这两个数据源都不用用户定义就可以自动匹配schema（元数据，即列信息），非常便利。JDBC数据源会自动执行filter优化，也就是直接在MySQL端进行过滤，从而减少数据传输。
```
CREATE TEMPORARY TABLE users USING jdbc
OPTIONS(driver "mysql" url "jdbc:mysql://userDB/users")

CREATE TEMPORARY TABLE logs
USING json OPTIONS (path "logs.json")

SELECT users.id, users.name, logs.message
FROM users JOIN logs WHERE users.id = logs.userId AND users.registrationDate > "2015-01-01"
```
在底层，JDBC数据源使用在4.4.1节中描述的PrunedFiltered-Scan接口，这个接口可以得到请求的列名以及在这些列上的predicates（例如equality、comparison或者IN clause）。在本例中，JDBC数据源会在MySQL中运行这样的代码：
```
SELECT users.id, users.name FROM users WHERE users.registrationDate > "2015-01-01"
```
在未来的Spark SQL版本中，我们也会在针对键值对的数据源添加predicate pushdown，例如HBase和Cassandra（支持有限的过滤类型）。

## 6 Evaluation
我们从两个方面来评估Spark SQL的性能：SQL查询处理性能和Spark程序性能。特别的，我们证明了Spark SQL中的增强框架不仅增加了更丰富的函数，而且对比之前的Spark-based SQL引擎有更大的性能提升。另外，对于Spark应用程序开发者来说，使用DataFrame API来进行开发效率远远大于原生的Spark API，同时使得Spark程序更加简化以及易于理解。最后，整合关系型和过程型的应用程序会比单独运行SQL或并向执行过程型代码运行的更快。

### 6.1 SQL Performance
我们使用Shark、Impala来和Spark SQL进行性能对比，使用AMPLab提供的big data benchmark。这个实验包含四种不同类型及参数的查询，具体为扫描（scan）、聚合（aggregation）、连接（join）和用户自定义MapReduce任务。本实验使用6个 EC2构成的集群（1主节点，5从节点），每个节点有4＆，30G内存以及一个800G的SSD硬盘，使用软件HDFS2.4，SPark1.3，Shark0.9.1 和Impala 2.1.1.数据使用Parquet格式的压缩数据，共110G大小。

图8显示了按查询类型分组，不同查询的结果对比。查询1-3对比了不同参数下的性能对比。with 1a, 2a, etc being the most selective and 1c, 2c, etc being the least selective and processing more data.（selective没看明白，更有选择性？）。查询4使用一个Python-based的Hive UDF来做实验，属于一个计算密集型任务（UDF没有在Impala中支持，所以就没有列出）。



![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure8.png?raw=true)

从所有的查询中来看，Spark SQL基本上会比Shark要快，而和Impala旗鼓相当。而Spark SQL和Shark的主要不同点是在Catalyst中的代码生成（code generation，见4.3.4节）模块，而这个减少了CPU的开销。而这个特性也是Spark SQL能在很多查询中，能和基于C++和LLVM的Impala比肩的原因。而和Impala差距最大的就是3a查询，而在这个查询中Impala使用了一个更好的join计划，because the selectivity of the queries makes one of the tables very small。

### 6.2 DataFrames vs. Native Spark Code 

除了可以运行SQL查询，Spark SQL也可以帮助非SQL开发者通过DataFrame API来编写简单并且高效的Spark代码。Catalyst可以在DataFrame操作中执行优化，而对应的手动编写的代码却不能执行优化，例如predicate pushdown（断言优化？）、管道操作（pipelining）、自动连接操作（join，所谓自动，指的是不管你是先过滤表，再连接，或者是先连接再过滤，Catalyst会自动帮你优化成先过滤再连接操作）。即使没有这些优化，使用DataFrame API也可以获得更高效的执行，因为DataFrame 操作会进行代码生成（code generation）。特别是针对Python编写的应用，因为Python原生就会比JVM要慢。

针对此项评估，我们针对分布式的聚合操作分别给出了两种Spark的实现，一种是Spark RDD，一种是Spakr DataFrame。使用的数据包含10亿个(a，b)这样的键值对，其中a是由10万个唯一值中的随机一个，使用的集群仍然是之前使用的5个节点的集群。我们通过计算每个a值对应的b值的平均值来评估时间消耗。首先，先看下使用Spark中Python API实现的版本：
```
sum_and_count = \
data.map(lambda x: (x.a, (x.b, 1))) \
.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
.collect()
[(x[0], x[1][0] / x[1][1]) for x in sum_and_count]
```
作为对比，实现相同功能的代码在DataFrame API中的实现只需要简单的一行，如下：
```
df.groupBy("a").avg("b")
```
![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure9.png?raw=true)

在图9中，可以看出DataFrame版本的实现比手动编写的Python性能要好12倍左右，同时也更加简洁。这是因为DataFrame的API只有逻辑计划是由Python构建的，而后的物理执行则是由原生SPark代码生成的JVM二进制代码，所以会执行的更加高效。实际上，DataFrame版本的代码会比Scala版本的执行效率高2倍，这主要归功于代码生成：在DataFrame版本中的代码避免了在手写代码中的对键值对的低效内存分配的情况。

## 6.3 Pipeline Performance

针对同时使用关系型和过程型代码的应用，DataFrame API也可以取得性能提升，用户可以在一个程序中编写所有的操作以及把这些操作整合成管道进行计算。例如，考虑一个包含两个Stage的管道操作：从一个text消息语料中提取一个子集，并计算出现最频繁的单词。尽管这个例子很简单，但是很多真实的管道操作也是类似的，如针对特定的人群来进行统计最受欢迎的tweet（推特）。

在此次试验中，我们在HDFS上生成了一个100亿条人工生成的数据集。每个记录平均包含10个从英语词典中抽取的单词。管道操作的第一个阶段（stage）使用一个关系型的filter也选择了大概90%的数据。第二个stage计算每个单词的出现次数。

![image](https://github.com/fansy1990/pic_bed/blob/master/Spark/Spark%20SQL%20Relational%20Data%20Processing%20in%20Spark/figure10.png?raw=true)

首先，我们使用下面的思路来实现该管道处理：先使用SQL 查询，然后使用Scala代码来实现。这会使用不同的查询引擎，例如SQL会使用Hive来查询，而Spark代码则使用Spark引擎。接着，我们通过DataFrame来实现这个管道操作。使用DataFrame的关系型操作符来实现filter，使用RDD API来执行单词计数。和第一种实现思路对比，第二个实现思路避免了SQL查询结果保存到HDFS文件上的操作（写文件降低效率），第二个思路是直接把filter和map的单词计数形成一个pipeline。图10对比了两种思路的性能和效率，从图中可以看出，DataFrame的执行方案的性能是另一种的2倍。


## 7 Research Applications
Spark SQL除了应用于真实的生产环境外，我们还发现一些研究者对把Spark SQL应用在实验项目上有很大兴趣。我们主要拿2个研究的项目来说明Catalyst的扩展性，一个是近似查询处理，另一个是genomics（基因学？）。

### 7.1 Generalized Online Aggregation
Zeng et al （应该是个人）在他们的项目中使用Catalyst来提升在线聚合的泛化能力（generality of online aggregation）。他们的工作使得在线聚合的执行能支持任意嵌套的聚合查询。同时，它可以计算所有数据的一部分，以此来使得用户可以直接看到查询执行的进度。这些部分的结果也包含正确率，这就使得用户可以在正确率达到一定程度后，结束查询。

为了在Spark SQL内部实现这个系统，作者添加了一个新的算子，这个算子对原始数据进行抽样，并返回多组抽样的结果。在调用transform函数的时候，其查询计划就会把原始所有数据的查询替换为逐个的抽样的子集的查询。

但是，在在线的环境中，只是简单的用抽样的数据来替换整个数据集并不能够计算出正确的结果。类似标准聚合操作需要使用有状态的副本来替换，也就是说需要同时考虑当前抽样子集数据以及之前子集的结果。再者，一些可能根据一个近似结果来进行元组过滤的操作一定需要被替换成能够考虑当前估算误差的版本。

所有这些transformations都可以通过Catalyst rule来表示，只需要修改算子树（operator tree）直到输出正确的在线结果。不是基于手痒数据的树片段会被这些规则忽略，同时可以使用标准的代码路径执行。使用Spark SQL作为基础，这个项目的作者可以通过大概2k行代码实现一个相当完整的原型。


### 7.2 Computational Genomics
在Computational Genomics中的一个参见的操作包含根据一个数值的偏移量来检查重合的区域（不是很理解）。这个问题可以表示为一个包含不相等表达式的join操作。例如，两个数据集a和b，其结构为（start LONG, end LONG）。那么区间join操作可以使用下面的表达式表示：
```
SELECT * FROM a JOIN b 
WHERE a.start < a.end 
    AND b.start < b.end
    AND a.start < b.start 
    AND b.start < a.end
```
在很多系统中，上面的查询都会被以一种非常低效的算法来执行，例如使用嵌套的循环join来执行。作为对比，一些专有的系统可以使用一个区间树（interval tree）来执行上面的查询SQL。在ADAM项目中的研究人员在Spark SQL中的一个版本中构建了一个特殊的planning rule（计划规则），以此使得上述查询SQL执行更加高效。allowing them to leverage the standard datamanipulation abilities alongside specialized processing code。这些实现代码大约有100行。

## 8 Related Work
**Programming Model（编程模型）**

在最初应用在大集群的一些系统中，他们的设计之初就是寻求一种可以整合关系型处理和过程型处理引擎的模型。在其中，Shark最像Spark SQL，都是在Spark引擎中执行，同时同样提供关系型查询和高级的分析过程的整合。Spark SQL比Shark更强的地方在于一个更多操作及更友好的API，也就是DataFrame。在DataFrame中一个查询可以被分割成多个模块（见Section3.4）。同时，DataFrame也支持在原生RDD上执行关系型查询，它还支持除了Hive外很多的数据源。

DryadLINQ 可以把用C#编写的查询进行编译，并发送到一个分布式的DAG执行引擎中，而这正是激发设计Spark SQL的初衷。LINO查询通常是关系型的，但是也可以直接在C#的类上操作。Spark SQL 超越DryadLINO的地方在于其提供了一个和常见的数据科学库中提供的接口，叫做DataFrame 接口。这个接口主要用于数据源和数据类型以及支持循环算法。

其他的系统只是在其内部使用一个关系统数据模型然后把过程型处理diam转换成UDFs。例如，Hive或Pig提供关系型查询语言，同时也使用了很多UDF接口。ASTERIX在内部使用一个半结构化的数据模型。Stratosphere也使用一个半结构化的模型，但是提供Scala、Java API，可以方便用户调用UDF。PIQL同样提供一个Scala DSL（领域特定语言）。和这些系统对比，SPark SQL在整合原生SPark应用方面显得更加紧密，因为用户可以直接在用户自定义的类上（如原生的Java或Python object）执行查询，而且开发者可以在一个语言中使用关系型和过程型API来进行编程。除此之外，通过Catalyst 优化器，Spark SQL不仅实现了优化（例如代码生成），而且还实现了其他功能（例如JSON的元数据识别以及机器学习中的数据类型），这在很多大数据计算框架中都没有。我们相信这些特性对于提供一个整合的、易用的大数据环境是非常必要的。

最后，DataFrame API既可以用于单机程序，也可以用于机器。和之前的API不同，SPark SQL通过一个关系型优化器来优化DataFrame的计算。

**Extensible Optimizers（可扩展的优化器）**
Catalyst优化器和其他优化器框架，如EXODUS、Casscades具有一样的目标。之前，人们一直认为优化器框架需要有一个领域特定语言来编写规则，同时要有一个“优化器编译器（Optimizer compiler）”来把这些规则翻译为能执行的代码。我们在这里最主要的改良就是使用函数式编程语言的标准特性来构建优化器，而使用这种方式同样可以提供和之前一样的功能（甚至更强），而且降低了维护和学习成本。编程语言中的这些高级特性使得Catalyst的设计受益很大，例如代码生成的实现就是使用quasiquotes（见Section 4.3.4），而据我们所知quasiquotes是实现这个任务的最简单以及具有较强组合性的方法之一。虽然可扩展性很难定量的评估，但是，Spark SQL在发布最开始的8个月中，已经有超过50个外部贡献者参与进来，这就很能说明问题了。

对于代码生成，LegoBase最近发表了一种使用Scala中生成式编程（generative programming）的方式来实现，而这种方式就可以替代使用quasiquotes，从而有更高的性能提升。

**Advanced Analytics（高级分析）**

Spark SQL基于最近的一些成果，才可以在大规模集群上进行一些高级分析算法，诸如专注于迭代算法和图分析的平台。就像MADlib一样，SPark SQL也非常愿意对用户提供分析的函数，但是MADlib和Spark SQL的实现是不一样的。在MADlib中只能使用Postgres中有限的UDF接口，而Spark SQL中的UDFs已经发展成为一个成熟的Spark程序。最后，一些技术，如Sinew和Invisible Loading都在需求在半结构化数据（例如JSON）查询上的优化。我们希望可以在Spark SQL中应用这些先进的技术。

## 9 Conclusion
我们呈现了一个在Apache Spark中的一个新模块，Spark SQL，提供了多种关系型处理的操作。Spark SQL使用声明式DataFrame API，进而提供关系型操作，以及提供诸如自动优化的好处，同时可以使得用户能把关系统操作和复杂的分析操作相混合成管道操作。它支持广泛的定制的大规模数据处理，包含半结构化数据，查询联邦（query federation）以及机器学习中的数据类型。为了使用这些特性，Spark SQL在内部实现了一个可扩展的优化器，Catalyst。Catalyst利用嵌入Scala编程语言的优势可以很方便的加入优化规则、数据源、数据类型。用户的反馈以及一些测试程序显示Spark SQL使得编写能整合关系型和过程型处理的数据管道操作更加简单和高效，同时提供对比之前的SQL -on -Spark 引擎更高的性能提升。

Spark SQL 在http://spark.apache.org 开源。

## 10 Acknowldegments

## 11 References



