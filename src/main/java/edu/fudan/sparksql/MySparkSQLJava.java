package edu.fudan.sparksql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class MySparkSQLJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        // 表1
        JavaRDD<Customer> customerRDD = spark.read()
                .textFile("/Users/bianlingfeng/Documents/customer.tbl")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\|");
                    Customer customer = new Customer();
                    customer.setC_CUSTKEY(parts[0]);
                    customer.setC_NAME(parts[1]);
                    customer.setC_ADDRESS(parts[2]);
                    customer.setC_NATIONKEY(parts[3]);
                    customer.setC_PHONE(parts[4]);
                    customer.setC_ACCTBAL(parts[5]);
                    customer.setC_MKTSEGMENT(parts[6]);
                    customer.setC_COMMENT(parts[7]);
                    return customer;
                });
        Dataset<Row> customerDF = spark.createDataFrame(customerRDD, Customer.class);
        customerDF.createOrReplaceTempView("customer");

        // 表2
        JavaRDD<Orders> ordersRDD = spark.read()
                .textFile("/Users/bianlingfeng/Documents/orders.tbl")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\|");
                    Orders orders = new Orders();
                    orders.setO_ORDERKEY(parts[0]);
                    orders.setO_CUSTKEY(parts[1]);
                    orders.setO_ORDERSTATUS(parts[2]);
                    orders.setO_TOTALPRICE(parts[3]);
                    orders.setO_ORDERDATE(parts[4]);
                    orders.setO_ORDERPRIORITY(parts[5]);
                    orders.setO_CLERK(parts[6]);
                    orders.setO_SHIPPRIORITY(parts[7]);
                    orders.setO_COMMENT(parts[8]);
                    return orders;
                });
        Dataset<Row> ordersDF = spark.createDataFrame(ordersRDD, Orders.class);
        ordersDF.createOrReplaceTempView("orders");

        // 表3
        JavaRDD<Lineitem> lineitemRDD = spark.read()
                .textFile("/Users/bianlingfeng/Documents/lineitem.tbl")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\\|");
                    Lineitem lineitem = new Lineitem();
                    lineitem.setL_ORDERKEY(parts[0]);
                    lineitem.setL_PARTKEY(parts[1]);
                    lineitem.setL_SUPPKEY(parts[2]);
                    lineitem.setL_LINENUMBER(parts[3]);
                    lineitem.setL_QUANTITY(parts[4]);
                    lineitem.setL_EXTENDEDPRICE(parts[5]);
                    lineitem.setL_DISCOUNT(parts[6]);
                    lineitem.setL_TAX(parts[7]);
                    lineitem.setL_RETURNFLAG(parts[8]);
                    lineitem.setL_LINESTATUS(parts[9]);
                    lineitem.setL_SHIPDATE(parts[10]);
                    lineitem.setL_COMMITDATE(parts[11]);
                    lineitem.setL_RECEIPTDATE(parts[12]);
                    lineitem.setL_SHIPINSTRUCT(parts[13]);
                    lineitem.setL_SHIPMODE(parts[14]);
                    lineitem.setL_COMMENT(parts[15]);
                    return lineitem;
                });
        Dataset<Row> lineitemDF = spark.createDataFrame(lineitemRDD, Lineitem.class);
        lineitemDF.createOrReplaceTempView("lineitem");

        // sql
//        spark.sql("select count(*) from lineitem").show();  // 6001215
//        spark.sql("select count(*) from orders").show();  // 1500000
//        spark.sql("select count(*) from customer").show();  // 150000
//        spark.sql("select * from lineitem limit 10").show();  // 150000


        // sql
        Dataset<Row> resDF = spark.sql("select l_orderkey, o_orderdate, sum(l_extendedprice) as revenue " +
                "from customer join orders join lineitem " +
                "where c_mktsegment = '" + args[0] + "' " +
                "and l_orderkey = o_orderkey " +
                "and c_custkey = o_custkey " +
                "and o_orderdate < '" + args[1] + "' " +
                "and l_shipdate > '" + args[2] + "' " +
                "group by l_orderkey,o_orderdate " +
                "order by revenue " +
                "desc " +
                "LIMIT " + args[3]);
        resDF.show();
    }
}
