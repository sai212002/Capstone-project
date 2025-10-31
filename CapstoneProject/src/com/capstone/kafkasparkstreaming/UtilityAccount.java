package com.capstone.kafkasparkstreaming;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class UtilityAccount {

        public static StructType accountSchema() {
                StructField acctNumberField = DataTypes.createStructField("accountNumber", DataTypes.IntegerType, false);
                StructField custIdFiled = DataTypes.createStructField("customerId", DataTypes.StringType, false);
                StructField acctTypeField = DataTypes.createStructField("accountType", DataTypes.StringType, false);
                StructField branchField = DataTypes.createStructField("branch", DataTypes.StringType, false);
                StructType accSchema = new StructType(
                                new StructField[] { acctNumberField, custIdFiled, acctTypeField, branchField });
                return accSchema;
        }

}