create database db_pro18;
use db_pro18;
create table tab1(cust_id string,total_bill string) row format delimited fields terminated by ',';load data local inpath '/home/cloudera/pigout1/part-m-00000' into table tab1;Add jar /home/cloudera/workspace/mytrimjar.jar/;create temporary function TRIM as 'mytrim';create table tab2(cust_id string,total_bill float);insert into table tab2 select cust_id,TRIM(total_bill) from tab1;create table tab3(cust_id string,total_bill_amount float);
insert into table tab3 select cust_id, SUM(total_bill) as total_bill_amount from tab2 group by cust_id order by total_bill_amount desc;create table class_A_customers(cust_id string,total_bill_amount float);
insert into table class_A_customers select * from tab3 where total_bill_amount > 1000000 ;create table class_B_customers(cust_id string,total_bill_amount float);
insert into table class_B_customers select * from tab3 where total_bill_amount < 1000000 AND total_bill_amount > 500000;create table class_C_customers(cust_id string,total_bill_amount float);
insert into table class_C_customers select * from tab3 where total_bill_amount < 500000 ;  
