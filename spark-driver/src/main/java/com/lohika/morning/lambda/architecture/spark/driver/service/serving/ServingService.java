package com.lohika.morning.lambda.architecture.spark.driver.service.serving;

import com.lohika.morning.lambda.architecture.spark.distributed.library.type.View;
import com.lohika.morning.lambda.architecture.spark.driver.context.AnalyticsSparkContext;
import java.util.Arrays;
import org.apache.spark.sql.DataFrame;
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

    public DataFrame getBatchView() {
        String[] tableNames = analyticsSparkContext.getSqlContext().tableNames();
        boolean isBatchViewPreCached = Arrays.stream(tableNames)
                                             .anyMatch(tableName -> tableName.equals(View.BATCH_VIEW.getValue()));

        if (!isBatchViewPreCached) {
            DataFrame batchView = analyticsSparkContext.getSqlContext().read().parquet(batchViewFilePath);
            // To trigger cache().
            batchView = batchView.cache();
            batchView.count();

            return batchView;
        } else {
            return analyticsSparkContext.getSqlContext().table(View.BATCH_VIEW.getValue());
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
