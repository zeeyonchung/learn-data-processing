package org.example.timestamp;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

/**
 * '2020-01-31 23:58:00.123 UTC' 데이터가 빅쿼리에 입력되는지 테스트
 * - TableFieldSchema.type이 DATETIME, TIMESTAMP
 * - Legacy Streaming API 사용
 * - DATETIME는 실패, TIMESTAMP는 성공해야 한다.
 * => DATETIME는 실패, TIMESTAMP는 성공함
 *
 * field3 type: DATETIME
 * field4 type: TIMESTAMP
 *
 * 실행 명령어:
 * mvn compile exec:java -Dexec.mainClass=org.example.timestamp.Main4 \
 * -Dexec.args="--project=<프로젝트ID> \
 * --gcpTempLocation=gs://<버킷Name>/temp/ \
 * --runner=DataflowRunner \
 * --region=northamerica-northeast1"
 */
public class Main4 {
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
                /*
                java.lang.RuntimeException: java.io.IOException: Insert failed:
                [{"errors":[{"debugInfo":"","location":"field3",
                "message":"Invalid datetime string \"2020-01-31 23:58:00.123 UTC\"",
                "reason":"invalid"}],"index":0}
                 */
//                new TableRow().set("field3", "2020-01-31 23:58:00.123 UTC"),
                new TableRow().set("field4", "2020-01-31 23:58:00.123adfsf UTC")));

        return pipeline.apply(data);
    }
    private static void writeToBigquery(PCollection<TableRow> rows) {
        WriteResult result = rows.apply("Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(TABLE_REFERENCE)
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        result.getFailedInserts().apply("Log Error",
                ParDo.of(new DoFn<TableRow, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        TableRow failedRow = c.element();
                        System.out.println(failedRow.get("field3") + ", " + failedRow.get("field4"));
                    }
                }));
    }
}
