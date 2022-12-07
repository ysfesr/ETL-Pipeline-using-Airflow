from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operator.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StagetoRedshiftOperator,
    LoadDataOperator,
    DataQualityOperator)

default_args = {
    'owner': 'elasery',
    'start_date': datetime(2021, 5, 12),
    'email': 'youssef.elassery@gmail.com',
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    's3_to_redshift',
    default_args=default_args,
    description='extract data from s3 and load them to redshift',
    schedule_interval='@once',
    max_active_runs=1
)


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Extract data from S3 to Staging tables on Redshift
stage_customer_statut = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'customer_statut',
    'cart_part8796',
    'customer_statut.csv')
stage_customer = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'customer',
    'cart_part8796',
    'customer.csv')
stage_orders = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'orders',
    'cart_part8796',
    'orders.csv')
stage_car_manufacturer = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'car_manufacturer',
    'cart_part8796',
    'car_manufacturer.csv')
stage_car = StagetoRedshiftOperator(
    'redshift', 'aws', 'car', 'cart_part8796', 'car.csv')
stage_supplier = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'supplier',
    'cart_part8796',
    'supplier.csv')
stage_brand = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'brand',
    'cart_part8796',
    'brand.csv')
stage_part_maker = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'part_maker',
    'cart_part8796',
    'part_maker.csv')
stage_part = StagetoRedshiftOperator(
    'redshift', 'aws', 'part', 'cart_part8796', 'part.csv')
stage_part_for_car = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'part_for_car',
    'cart_part8796',
    'part_for_car.csv')
stage_part_supplier = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'part_supplier',
    'cart_part8796',
    'part_supplier.csv')
stage_part_in_order = StagetoRedshiftOperator(
    'redshift',
    'aws',
    'part_in_order',
    'cart_part8796',
    'part_in_order.csv')


# Transform and load data from staging tables to fact and dimension tables
query = '''SELECT a.part_in_order_id, d.brand_id, e.car_id, f.car_manufacturer_id, b.customer_id, a.order_id, c.part_id,\
       d.part_maker_id, a.part_supplier_id, d.supplier_id, a.actual_sale_price, a.quantity\
    FROM Part_in_Order a\
    JOIN Orders b USING(order_id)\
    JOIN Part_Supplier c USING(part_supplier_id)\
    JOIN Part d USING(part_id)\
    JOIN Part_for_Car e USING(part_id)\
    JOIN Car f USING(car_id)'''

columns = "fact_id,brand_id,car_id,car_manufacturer_id,customer_id, order_id,part_id, part_maker_id, part_supplier_id, supplier_id, actual_sale_price, quantity"

load_fact_part_in_order = LoadDataOperator(
    'redshift', 'factpart_in_order', columns, query)
load_customer = LoadDataOperator(
    'redshift',
    'dimcustomer',
    stmt='select * from customer')
load_orders = LoadDataOperator(
    'redshift',
    'dimorders',
    stmt='select * from orders')
load_car_manufacturer = LoadDataOperator(
    'redshift',
    'dimcar_manufacturer',
    stmt='select * from car_manufacturer')
load_car = LoadDataOperator('redshift', 'dimcar', stmt='select * from car')
load_supplier = LoadDataOperator(
    'redshift',
    'dimsupplier',
    stmt='select * from supplier')
load_brand = LoadDataOperator(
    'redshift',
    'dimbrand',
    stmt='select * from brand')
load_part_maker = LoadDataOperator(
    'redshift',
    'dimpart_maker',
    stmt='select * from part_maker')
load_part = LoadDataOperator('redshift', 'dimpart', stmt='select * from part')
load_part_for_car = LoadDataOperator(
    'redshift',
    'dimpart_for_car',
    stmt='select * from part_for_car')
load_part_supplier = LoadDataOperator(
    'redshift',
    'dimpart_supplier',
    stmt='select * from part_supplier')


# Check the quality of data
check_quality = DataQualityOperator(
    'redshift',
    'factpart_in_order',
    "select count(*) from factpart_in_order")


start_operator >> [
    stage_customer_statut,
    stage_customer,
    stage_orders,
    stage_car_manufacturer,
    stage_car,
    stage_supplier,
    stage_brand,
    stage_part_maker,
    stage_part,
    stage_part_for_car,
    stage_part_supplier,
    stage_part_in_order]

stage_customer_statut >> load_fact_part_in_order
stage_customer >> load_fact_part_in_order
stage_orders >> load_fact_part_in_order
stage_car_manufacturer >> load_fact_part_in_order
stage_car >> load_fact_part_in_order
stage_supplier >> load_fact_part_in_order
stage_brand >> load_fact_part_in_order
stage_part_maker >> load_fact_part_in_order
stage_part >> load_fact_part_in_order
stage_part_for_car >> load_fact_part_in_order
stage_part_supplier >> load_fact_part_in_order
stage_part_in_order >> load_fact_part_in_order

load_fact_part_in_order >> [
    load_fact_part_in_order,
    load_customer,
    load_orders,
    load_car_manufacturer,
    load_car,
    load_supplier,
    load_brand,
    load_part_maker,
    load_part,
    load_part_for_car,
    load_part_supplier]

check_quality << [
    load_fact_part_in_order,
    load_customer,
    load_orders,
    load_car_manufacturer,
    load_car,
    load_supplier,
    load_brand,
    load_part_maker,
    load_part,
    load_part_for_car,
    load_part_supplier]
