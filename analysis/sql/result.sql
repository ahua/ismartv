ADD JAR /usr/lib/hive/lib/zookeeper.jar;
ADD JAR /usr/lib/hive/lib/hive-hbase-handler-0.10.0-cdh4.4.0.jar;
ADD JAR /usr/lib/hive/lib/guava-11.0.2.jar;

create table daily_result(
   day_device string,
   total_user int comment 'a',
   new_user int comment 'b',
   active_user int comment 'c',
   vod_user int comment 'd',
   vod_display_times int comment 'e',
   vod_avg_duration float comment 'f',
   vod_activation_rate float comment 'g',
   system_on_rate float comment 'h',
   app_activation_rate float comment 'i',
   smart_activation_rate float comment 'j'
) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,a:a,a:b,a:c,a:d,a:e,a:f,a:g,a:h,a:i,a:j")
TBLPROPERTIES ("hbase.table.name" = "daily_result");
