
# Spark SQL： Relational Data Processing in Spark
[Toc]
## Abstract
Spark SQL是Apache Spark中的一个新模块，其在Spark的函数式编程API中引入了关系型处理能力。基于在Shark上的经验，Spark SQL可以使得Spark开发者获得在关系型处理过程中才能得到的好处（如声明式的查询，存储优化），同时可以使SQL开发者可以在Spark中调用复杂的分析库（分析函数库，如机器学习）。和之前的系统对比，Spark SQL添加了两个主要功能：第一，通过声明式的DataFrame API以及过程处理代码，在关系型和过程型处理过程中增加一个更加紧密的交互方式；第二，新增了一个高度可扩展的优化器，Catalyst（此优化器通
