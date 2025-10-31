package com.capstone.kafkasparkstreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.*;
//import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaSparkStreamWithBranchingData {

        public static void main(String[] args) {

                SparkSession spark = SparkSession.builder().appName("data-set-streaming-app").master("local[*]").getOrCreate();
                spark.sparkContext().setLogLevel("WARN");
                Dataset<Row> ds1 = spark.readStream().format("kafka").option("kafka.bootstrap.servers", 
                                "localhost:9092")
                                .option("subscribe", "account-topic").load().selectExpr("CAST(value AS STRING)");
                Dataset<Row> jsonDS=ds1.select(from_json(col("value"),UtilityAccount.accountSchema()).as("data"));
                Dataset<Row> accDS=jsonDS.select("data.*");
                Dataset<Row> CADS=accDS.where("accountType='CA'").select("accountNumber","customerId","branch");
                Dataset<Row> SBDS=accDS.where("accountType='SB'").select("accountNumber","customerId","branch");
                Dataset<Row> RDDS=accDS.where("accountType='RD'").select("accountNumber","customerId","branch");
                Dataset<Row> LoanDS=accDS.where("accountType='Loan'").select("accountNumber","customerId","branch");
                                /*
                                 * Dataset<Row> otherDS=accDS.
                                 * where("accountType!='Developer' and designation!='Architect' and designation!='Accountant'"
                                 * ). select("id","name");
                                 */
                 StreamingQuery CAStreamingQuery = null;
                 StreamingQuery SBStreamingQuery = null;
                 StreamingQuery RDStreamingQuery = null;
                 StreamingQuery LoanStreamingQuery = null;
                 //StreamingQuery otherStreamingQuery = null;
                try {
                        CAStreamingQuery = CADS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("csv")
                                        .option("checkpointLocation","c:/capstone/ca")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-CA-out");
                        SBStreamingQuery = SBDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("json")
                                        .option("checkpointLocation","c:/capstone/sb")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-SB-out");
                        
                        RDStreamingQuery = RDDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                        .format("avro")
                                        .option("checkpointLocation","c:/capstone/rd")
                                        .option("header", true)
                                        .start("c:/structuredstream-kafka-RD-out");
                        LoanStreamingQuery = LoanDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                            .format("parquet")
                            .option("checkpointLocation","c:/capstone/loan")
                            .option("header", true)
                            .start("c:/structuredstream-kafka-Loan-out");
                        
                         accDS.groupBy(col("branch"),col("accountType")).count()
                            .withColumnRenamed("count","number_of_accounts")
            .writeStream()
            .foreachBatch((batchDF,batchId)->{
                    batchDF.write()
                    .format("jdbc")
                    .option("url","jdbc:mysql://localhost:3306/mysql")
                    .option("dbtable", "analytics_table_tb1")
                    .option("user", "root")
                    .option("password","Password@1")
                    .mode(SaveMode.Append).save();
            })
            .option("checkpointLocation","C:/capstone/checkpoints/agg")
            .outputMode("update")
            .start();
                                        /*
                                         * otherStreamingQuery =
                                         * otherDS.writeStream().trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
                                         * .format("parquet") .option("checkpointLocation","c:/othercheckpoint")
                                         * .option("header", true) .start("c:/structuredstream-kafka-other-out");
                                         */
                        System.out.println("streaming started");

                        Thread.sleep(10 * 60 * 1000);
                        CAStreamingQuery.stop();
                        SBStreamingQuery.stop();
                        RDStreamingQuery.stop();
                        LoanStreamingQuery.stop();
                        
                        
                } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                } catch (TimeoutException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }

                System.out.println("streaming stopped");
        }

}
