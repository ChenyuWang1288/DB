# MiniSQL阶段报告5

——Executor

> 第七小组

## 3.1 实验概述

Executor（执行器）的主要功能是根据解释器（Parser）生成的语法树，通过Catalog Manager 提供的信息生成执行计划，并调用 Record Manager、Index Manager 和 Catalog Manager 提供的相应接口进行执行，最后通过执行上下文`ExecuteContext`将执行结果返回给上层模块。

## 3.2 函数实现

- `ExecuteEngine::ExecuteCreateDatabase(*ast, *context)`

  - 思路

    - 先找是否已经有同名数据库
    - 建立数据库，插入`unordered_map dbs_`中，记录当前的数据库

  - 测试

    - 插入前

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625231015144.png" alt="image-20220625231015144" style="zoom:50%;" /> 

    - 插入后文件中新建了db0，databasefile.txt中已经记录了已有的数据库db0

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625231041511.png" alt="image-20220625231041511" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteDropDatabase(*ast, *context)`

  - 思路

    - 在`dbs_`中找到该数据库，删除
    - 在文件中删除

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626132544259.png" alt="image-20220626132544259" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteShowDatabases(*ast, *context)`

  - 思路

    - 打印`dbs_`中已有的数据库

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625231220056.png" alt="image-20220625231220056" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteUseDatabase(*ast, *context)`

  - 思路

    - 找到database并把它作为`current_db_`

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625231142711.png" alt="image-20220625231142711" style="zoom: 50%;" /> 

- `ExecuteEngine::ExecuteShowTables(*ast, *context)`

  - 思路

    - 在`Current Database`中的`catalog_mgr_`中的函数`GetTables()`得到当前数据库中的表名，输出。

  - 测试

    - 此时打印该database里已经有的tablename

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000527257.png" alt="image-20220626000527257" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteCreateTable(*ast, *context)`

  - 思路
    - 遍历语法树将column和type的信息收集，形成一个列的`vector`
    - 注意`primary key`，该列不能有重复的元素
    - 在`Current database`中的`catalog_mgr_`中调用`CreateTable()`来新建一个table
  - 测试
    - ![image-20220625233618690](C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625233618690.png)

- `ExecuteEngine::ExecuteDropTable(*ast, *context)`

  - 思路

    - 得到需要被drop的table name
    - 调用`current database`中的`catalog_mgr_`中的`DropTable()`函数来drop

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626132452634.png" alt="image-20220626132452634" style="zoom: 50%;" /> 

- `ExecuteEngine::ExecuteShowIndexes(*ast, *context)`

  - 思路

    - 用`GetTableIndexes()`来得到表的索引并且打印

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625234056835.png" alt="image-20220625234056835" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteCreateIndex(*ast, *context)`

  - 思路

    - 检查是否在唯一键上建立索引
    - 调用`current database`中的`catalog_mgr_`中的`CreateIndex()`函数

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625233753350.png" alt="image-20220625233753350" style="zoom: 50%;" /> 

- `ExecuteEngine::ExecuteDropIndex(*ast, *context)`

  - 思路

    - 在现有的tables中寻找同名的index
    - 调用`current database`中的`catalog_mgr_`中的`DropIndex()`函数

  - 测试

    - 此时把之前新建的在b上的index删除

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000440170.png" alt="image-20220626000440170" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteSelect(*ast, *context)`

  - 思路

    - 遍历语法树得到需要被select的columns
    - 通过语法树得到每一个条件，每得到一个条件返回一个`vector<RowId>`最后对结果做布尔运算
      - 用`table iterator`结合需要找的columns找到每个符合要求的`field`然后输出结果	

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625235309152.png" alt="image-20220625235309152" style="zoom:50%;" /> 

    - 带条件的查找：

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625235345534.png" alt="image-20220625235345534" style="zoom:50%;" /> 

    - 嵌套条件

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625235428121.png" alt="image-20220625235428121" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteInsert(*ast, *context)`

  - 思路

    - 调用`current database`中的`catalog_mgr_`中的`GetTable()`函数找到要被插入的表
    - 通过语法树得到需要被插入的一条记录的type以及内容，与该表的column做检查
    - 检查unique以及primary key
    - 调用`current table`中的`GetTableHeap()`中的`InsertTuple()`函数插入一条记录
    - 更新index

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625234156843.png" alt="image-20220625234156843" style="zoom:50%;" /> 

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625234853211.png" alt="image-20220625234853211" style="zoom:50%;" /> 

    - Unique约束：

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625234950055.png" alt="image-20220625234950055" style="zoom:50%;" /> 

    - Primary key 约束：

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625235024680.png" alt="image-20220625235024680" style="zoom:50%;" /> 

    - 格式错误的情况：

      <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220625235119318.png" alt="image-20220625235119318" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteDelete(*ast, *context)`

  - 思路

    - 调用`current database`中的`catalog_mgr_`中的`GetTable()`函数找到要被删除记录的表
    - 通过类似于`select`中的方法来找到需要被删除的`vector<RowId>`然后通过迭代器删除
    - 删除index

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000646953.png" alt="image-20220626000646953" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteUpdate(*ast, *context)`

  - 思路

    - 与`select`、`insert`、`delete`中原理一致
    - 更新也需判断unique和primary key等约束条件
    - 更新index

  - 测试

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000008950.png" alt="image-20220626000008950" style="zoom:50%;" /> 

    此时select，可知表中内容为：

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000031634.png" alt="image-20220626000031634" style="zoom:50%;" /> 

    已经成功update。

    与primary key冲突的update：

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000149758.png" alt="image-20220626000149758" style="zoom:50%;" /> 

    与unique冲突的update：

    <img src="C:\Users\17260\AppData\Roaming\Typora\typora-user-images\image-20220626000245391.png" alt="image-20220626000245391" style="zoom:50%;" /> 

- `ExecuteEngine::ExecuteExecfile(*ast, *context)`

  - 思路
    - 与`main.cpp`中的内容相同，只是执行文件中的每一行指令
  - 测试
    - 最后验收的时候展示

- `ExecuteEngine::ExecuteQuit(*ast, *context)`

  - 思路
    - `context->flag_quit_ = true`
  - 测试
    - 命令行中输入`quit;`回车后程序结束

