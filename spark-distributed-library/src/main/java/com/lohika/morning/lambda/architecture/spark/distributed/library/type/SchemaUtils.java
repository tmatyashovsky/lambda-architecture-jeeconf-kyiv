package com.lohika.morning.lambda.architecture.spark.distributed.library.type;

import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column.COUNT;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column.HASH_TAG;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SchemaUtils {

    private SchemaUtils() {}

    public static StructType generateSchemaStructure() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(HASH_TAG.getValue(), DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(COUNT.getValue(), DataTypes.LongType, true));

        return DataTypes.createStructType(fields);
    }

}
