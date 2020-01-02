![Datax-logo](https://github.com/alibaba/DataX/blob/master/images/DataX-logo.jpg)

**基于DataX自定义插件，已自定义插件：** 

- ##### [otsplusreader](https://github.com/codingdebugallday/DataX/blob/master/otsplusreader/doc/otsplusreader.md)

        可以对ots的字段进行加密解密操作

- ##### [hdfsplusreader](https://github.com/codingdebugallday/DataX/blob/master/hdfsplusreader/doc/hdfsplusreader.md)

        由于hdfsreader插件是基于hdfs上的文件，不能自定义sql，因此开发了hdfsplusreader插件,
        通过Shell执行自定义Hive查询SQL，写入临时表(ORC)，再将临时表数据给到DataX，最后删除。

- ##### [hdfspluswriter](https://github.com/codingdebugallday/DataX/blob/master/hdfspluswriter/doc/hdfspluswriter.md)

        在hdfswriter基础上，做了增强处理: 
        1. 增加preSql，postSql，跟mysqlWriter中的preSql，postSql一样
        2. 增加delimsReplacement，dropImportDelims，对字段中的\n、\r以及\01处理，跟sqoop一样
        

# DataX

DataX 是阿里巴巴集团内被广泛使用的离线数据同步工具/平台，实现包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、DRDS 等各种异构数据源之间高效的数据同步功能。



# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。



# DataX详细介绍

##### 请参考：[DataX-Introduction](https://github.com/alibaba/DataX/blob/master/introduction.md)



# Quick Start

##### Download [DataX下载地址](http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz)

##### 请点击：[Quick Start](https://github.com/alibaba/DataX/blob/master/userGuid.md)


# 我要开发新的插件
请点击：[DataX插件开发宝典](https://github.com/alibaba/DataX/blob/master/dataxPluginDev.md)

# License

This software is free to use under the Apache License [Apache license](https://github.com/alibaba/DataX/blob/master/license.txt).

# 
请及时提出issue给我们。请前往：[DataxIssue](https://github.com/alibaba/DataX/issues)
