package com.lohika.morning.lambda.architecture.spark.driver.service.serving;

import com.lohika.morning.lambda.architecture.spark.distributed.library.type.View;
import com.lohika.morning.lambda.architecture.spark.driver.context.AnalyticsSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Table;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ServingService implements InitializingBean {

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @Value("${batch.view.file.path}")
    private String batchViewFilePath;

    @Value("${batch.view.force.precache}")
    private Boolean forceBatchViewPreCache;

    public Dataset<Row> getBatchView() {
        Dataset<Table> tables = analyticsSparkContext.getSparkSession().catalog().listTables();
        Dataset<Table> batchViewTable = tables.filter(tables.col("name").equalTo(View.BATCH_VIEW.getValue()));
        boolean isBatchViewPreCached = batchViewTable.count() == 1;

        if (!isBatchViewPreCached) {
            Dataset<Row> batchView = analyticsSparkContext.getSparkSession().read().parquet(batchViewFilePath);
            // To trigger cache().
            batchView.cache();
            batchView.count();

            return batchView;
        } else {
            return analyticsSparkContext.getSparkSession().table(View.BATCH_VIEW.getValue());
        }
    }

    public void setBatchViewFilePath(String batchViewFilePath) {
        this.batchViewFilePath = batchViewFilePath;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (forceBatchViewPreCache) {
            this.getBatchView();
        }
    }

}
