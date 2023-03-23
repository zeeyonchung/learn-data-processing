package org.example.timestamp;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

/**
 * '2020-01-31 23:58:00.123' 데이터가 빅쿼리에 입력되는지 테스트
 * - TableFieldSchema.type이 DATETIME, TIMESTAMP
 * - DATETIME도 성공, TIMESTAMP도 성공해야 한다.
 * => 둘 다 성공함 (field3에는 '2020-01-31T23:58:00.123000', field4에는 '2020-01-31 23:58:00.123000 UTC' 입력됨)
 *
 * field3 type: DATETIME
 * field4 type: TIMESTAMP
 *
 * 실행 명령어:
 * mvn compile exec:java -Dexec.mainClass=org.example.timestamp.Main3 \
 * -Dexec.args="--project=<프로젝트ID> \
 * --gcpTempLocation=gs://<버킷Name>/temp/ \
 * --runner=DataflowRunner \
 * --region=northamerica-northeast1"
 */
public class Main3 {
    private static final TableReference TABLE_REFERENCE = new TableReference()
            .setProjectId("project_id")
            .setDatasetId("sample_dataset")
            .setTableId("sample_table");

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> tableRows = createTableRow(pipeline);
        writeToBigquery(tableRows);

        pipeline.run().waitUntilFinish();
    }

    public static PCollection<TableRow> createTableRow(Pipeline pipeline) {
        Create.Values<TableRow> data = Create.of(Arrays.asList(
                new TableRow().set("field3", "2020-01-31 23:58:00.123"),
                new TableRow().set("field4", "2020-01-31 23:58:00.123")));

        return pipeline.apply(data);
    }
    private static void writeToBigquery(PCollection<TableRow> rows) {
        WriteResult result = rows.apply("Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(TABLE_REFERENCE)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        result.getFailedStorageApiInserts().apply("Log Error",
                ParDo.of(new DoFn<BigQueryStorageApiInsertError, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        BigQueryStorageApiInsertError error = c.element();
                        System.out.println(error.getErrorMessage());
                    }
                }));
    }
}
