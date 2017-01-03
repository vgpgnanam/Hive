# Hive

# create database

create database if not exists gnanaprakasam;

# create table in Hive

create table categories(
category_id int,
category_department_id string,
category_name string)
row format delimited fields terminated by ','
stored as textfile;

create table customers(
customer_id  int,
customer_fname string,
customer_lname string,
customer_email string,
customer_password string,
customer_street string,
customer_city string,
customer_state string,
customer_zipcode string)
row format delimited fields terminated by ','
stored as textfile;
 
select user, file_priv from mysql.user;
update mysql.user set file_priv = 'Y' where user = 'retail_dba';
commit;

#Restart the mysql service
service mysqld restart

 select * from categories into outfile '/tmp/categories01.psv' fields terminated by '|' lines terminated by '/n';

 select * from customers into outpfile '/tmp/customers.psv' fields terminated by '|' lines terminated by '/n';

 /home/gnanaprakasam/data/retail_db/categories

 load data local inpath '/home/gnanaprakasam/data/retail_db/categories' overwrite into table categories;

 load data local inpath '/home/gnanaprakasam/data/retail_db/customers' into table customers;

hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/categories
hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/customers

Original - /user/gnanaprakasam/data/retail_db/categories
Copy for load - /user/gnanaprakasam/data/retail_db_bkp/categories

Original - /user/gnanaprakasam/data/retail_db/customers
Copy for load - /user/gnanaprakasam/data/retail_db_bkp/customers

hadoop fs -cp /user/gnanaprakasam/data/retail_db/categories /user/gnanaprakasam/data/retail_db_bkp/
hadoop fs -cp /user/gnanaprakasam/data/retail_db/customers /user/gnanaprakasam/data/retail_db_bkp/

load data inpath '/user/gnanaprakasam/data/retail_db_bkp/categories' into table categories;
load data inpath '/user/gnanaprakasam/data/retail_db_bkp/customers' into table customers;

hive> load data inpath '/user/gnanaprakasam/data/retail_db_bkp/categories' into table categories;
Loading data to table gnanaprakasam.categories
Failed with exception org.apache.hadoop.security.AccessControlException: User does not belong to hdfs
        at org.apache.hadoop.hdfs.server.namenode.FSDirAttrOp.setOwner(FSDirAttrOp.java:88)
        at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.setOwner(FSNamesystem.java:1708)
        at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.setOwner(NameNodeRpcServer.java:821)
        at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.setOwner(ClientNamenodeProtocolServerSideTranslatorPB.java:472)
        at org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)
        at org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:640)
        at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:982)
        at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2313)
        at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2309)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1724)
        at org.apache.hadoop.ipc.Server$Handler.run(Server.java:2307)
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.MoveTask

# create partitioned table

create external table orders_partition(
order_id int,
order_date string,
order_customer_id int,
order_status string
)
partitioned by (order_month string)
row format delimited fields terminated by ','
stored as textfile;

hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/orders_partition

insert overwrite table orders_partition partition (order_month)
select order_id, order_date, order_customer_id, order_status, substr(order_date, 1, 7) order_month from orders;

set hive.exec.dynamic.partition;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode;
set hive.exec.dynamic.partition.mode=nonstrict;

create external table order_items_partition(
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float)
partitioned by (order_item_order_month string)
row format delimited fields terminated by ','
stored as textfile;

insert overwrite table order_items_partition partition (order_item_order_month)
select oi.order_item_id, oi.order_item_order_id, o.order_date, oi.order_item_product_id, oi.order_item_quantity, 
oi.order_item_subtotal, oi.order_item_product_price, substr(o.order_date, 1, 7) order_item_order_month from orders o join order_items oi
on o.order_id = oi.order_item_order_id;

hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/order_items_partition

# Create bucket tables

create table order_bucket(
order_id int,
order_date string,
order_customer_id int,
order_status string)
clustered by (order_id) into 16 buckets
row format delimited fields terminated by ','
stored as textfile;

set hive.enforce.bucketing;
set hive.enforce.bucketing=true;

insert overwrite table order_bucket
select order_id, order_date, order_customer_id, order_status from orders;

hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/order_bucket


create table order_items_bucket(
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
clustered by (order_item_order_id) into 16 buckets
row format delimited fields terminated by ','
stored as textfile;


insert overwrite table order_items_bucket
select oi.order_item_id, oi.order_item_order_id, o.order_date, oi.order_item_product_id, oi.order_item_quantity, 
oi.order_item_subtotal, oi.order_item_product_price from orders o join order_items oi
on o.order_id = oi.order_item_order_id;

hdfs://nn01.itversity.com:8020/apps/hive/warehouse/gnanaprakasam.db/order_items_bucket
