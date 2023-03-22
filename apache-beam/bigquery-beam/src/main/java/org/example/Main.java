package org.example;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
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
 * Apache Beam에서 BigQuery로 데이터 입력
 * field1 type: String
 * field2 type: Integer
 *
 * 실행 명령어:
 * mvn compile exec:java -Dexec.mainClass=org.example.Main \
 * -Dexec.args="--project=<프로젝트ID> \
 * --gcpTempLocation=gs://<버킷Name>/temp/ \
 * --runner=DataflowRunner \
 * --region=northamerica-northeast1"
 */
public class Main {
    private static final TableReference TABLE_REFERENCE = new TableReference()
            .setProjectId("project_id")
            .setDatasetId("sample_dataset")
            .setTableId("sample_table");

    public static PCollection<TableRow> createTableRow(Pipeline pipeline) {
        Create.Values<TableRow> data = Create.of(Arrays.asList(
                new TableRow().set("field1", "Hello").set("field2", 123),
                new TableRow().set("field1", "World").set("field2", "this must cause error")));

        return pipeline.apply(data);
    }

    public static void writeToTable(PCollection<TableRow> rows) {
        WriteResult result = rows.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(TABLE_REFERENCE)
                        .withMethod(Method.STORAGE_WRITE_API)
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND));

        result.getFailedStorageApiInserts().apply("Log Error",
                ParDo.of(new DoFn<BigQueryStorageApiInsertError, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        BigQueryStorageApiInsertError error = c.element();
                        System.out.println(error.getErrorMessage());
                        System.out.println(error.getRow().get("field1"));
                    }
                }));
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> rows = createTableRow(pipeline);
        writeToTable(rows);
        pipeline.run().waitUntilFinish();
    }
}
