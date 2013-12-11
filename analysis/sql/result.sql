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
   c float comment '周VOD激活率',
   d float comment '周应用激活率',
   e float comment '周智能激活率',
   f int comment '日均活跃用户数',
   g int comment '日均VOD用户数',
   h int comment '日均VOD播放次数',
   i float comment '日均户均时长',
   j float comment '日均VOD激活率',
   k float comment '日均应用激活率',
   l float comment '日均智能激活率'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d,a:e,a:f,a:g,a:h,a:i,a:j,a:k,a:l")
TBLPROPERTIES ("hbase.table.name" = "weekly_result");

create table monthly_result(
   day_device string,
   a int comment '月活跃用户数',
   b int comment '月VOD用户数',
   c float comment '月VOD激活率',
   d float comment '日均活跃用户数',
   e float comment '日均VOD用户数',
   f int comment '日均VOD户均时长',
   g int comment '日均VOD户均访次',
   h int comment '日均VOD激活率',
   i float comment '日均开机率',
   j float comment '首次缓冲3秒内（含）占比',
   k float comment '每次播放卡顿2次内占比',
   l float comment '月应用激活率',
   m float comment '月智能激活率'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d,a:e,a:f,a:g,a:h,a:i,a:j,a:k,a:l,a:m")
TBLPROPERTIES ("hbase.table.name" = "monthly_result");

