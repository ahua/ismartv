ADD JAR /usr/lib/hive/lib/zookeeper.jar;
ADD JAR /usr/lib/hive/lib/hive-hbase-handler-0.10.0-cdh4.5.0.jar;
ADD JAR /usr/lib/hive/lib/guava-11.0.2.jar;

create table daily_result(
   day_device string,
   a int comment '累计用户数',
   b int comment '新增用户数',
   c int comment '活跃用户数',
   d int comment 'VOD用户数',
   e int comment 'VOD播放次数',
   f float comment 'VOD用户播放总时长',
   g float comment '应用激活率',
   h float comment '智能激活率'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d,a:e,a:f,a:g,a:h")
TBLPROPERTIES ("hbase.table.name" = "daily_result");

create table weekly_result(
   day_device string,
   a int comment '周活跃用户数',
   b int comment '周VOD用户数',
   c float comment '周应用用户数',
   d float comment '周智能用户数'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d")
TBLPROPERTIES ("hbase.table.name" = "weekly_result");

create table monthly_result(
   day_device string,
   a int comment '月活跃用户数',
   b int comment '月VOD用户数',
   c float comment '月应用激活率',
   d float comment '月智能激活率',
   e float comment '首次缓冲3秒内（含）占比',
   f float comment '每次播放卡顿2次内占比'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d,a:e,a:f")
TBLPROPERTIES ("hbase.table.name" = "monthly_result");

