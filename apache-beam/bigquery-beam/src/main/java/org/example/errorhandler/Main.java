package org.example.errorhandler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 실행 명령어
 * mvn compile exec:java -Dexec.mainClass=org.example.timestamp.Main1 \
 * -Dexec.args="--project=<프로젝트ID> \
 * --gcpTempLocation=gs://<버킷Name>/temp/ \
 * --runner=DataflowRunner \
 * --region=northamerica-northeast1"
 */
public class Main {
    public interface MyOption extends PipelineOptions {
        String getProjectId();
        void setProjectId(String projectId);
        String getSubscription();
        void setSubscription(String subscription);
    }

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final Schema schema = Schema.of(
            Schema.Field.of("StringField", Schema.FieldType.STRING),
            Schema.Field.of("IntField", Schema.FieldType.INT16)
    );

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(new ExceptionHandler());

        MyOption options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(MyOption.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadFromPubsub",
                        PubsubIO.readMessages()
                                .fromSubscription(String.format("projects/%s/subscriptions/%s",
                                        options.getProjectId(), options.getSubscription()))
                )
                .apply("GetPayload",
                        ParDo.of(new DoFn<PubsubMessage, String>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) throws IOException {
                                PubsubMessage message = c.element();
                                byte[] payload = message.getPayload();
                                c.output(new String(payload, StandardCharsets.UTF_8));
                            }
                        })
                )
                .apply("ParseJson",
                        JsonToRow.withSchema(schema)
                )
                .apply("PrintValue",
                        ParDo.of(new DoFn<Row, Void>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                Row row = c.element();
                                List<Object> values = row.getValues();
                                values.forEach((v) -> LOG.debug(v.toString()));
                            }
                        })
                );

        pipeline.run();
    }
}
