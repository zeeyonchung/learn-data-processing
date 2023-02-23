package my.examples.playground4;

import java.util.Map;
import my.examples.playground4.PubsubUtils.TopicSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * To execute this pipeline, run the command below.
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass=my.examples.playground4.FileToFilePipeline \
 * -Dexec.args="--inputTopics=user" -Pdirect-runner
 * }</pre>
 */
public class FileToFilePipeline {
  public interface Option extends PipelineOptions {
    String getInputTopics();
    void setInputTopics(String value);
  }

  public static void main(String[] args) {
    Option options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Option.class);
    run(options);
  }

  private static void run(Option options) {
    Pipeline p = Pipeline.create(options);

    FilePcollectionFetchers pcollectionFetchers = FilePcollectionFetchers.create(options);
    Map<TopicSchema, PCollection<Row>> pcollections = pcollectionFetchers.pcollections(p);

    for (Map.Entry<TopicSchema, PCollection<Row>> entry : pcollections.entrySet()) {
      TopicSchema ts = entry.getKey();
      PCollection<Row> input = entry.getValue();

      input
          .setRowSchema(ts.schema)
          .apply(EmailEncrypt.of())
          .setRowSchema(ts.schema)
          .apply(FileDataApplier.of(ts.outputFileName));
    }

    p.run().waitUntilFinish();
  }
}
