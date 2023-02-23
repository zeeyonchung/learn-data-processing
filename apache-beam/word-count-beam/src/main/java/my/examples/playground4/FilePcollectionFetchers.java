package my.examples.playground4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import my.examples.playground4.PubsubUtils.TopicSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

class FilePcollectionFetchers {
  private FileToFilePipeline.Option options;

  private FilePcollectionFetchers(FileToFilePipeline.Option options) {
    this.options = options;
  }

  public static FilePcollectionFetchers create(FileToFilePipeline.Option options) {
    return new FilePcollectionFetchers(options);
  }

  public Map<TopicSchema, PCollection<Row>> pcollections(Pipeline p) {
    Map<TopicSchema, PCollection<Row>> result = new HashMap<>();
    List<TopicSchema> readSourceSchemas = buildSchemas(options.getInputTopics());

    for (TopicSchema ts : readSourceSchemas) {
      PCollection<Row> rows = p
          .apply(TextIO.read().from(ts.inputFilePath))
          .apply(ParseRows.withSchema(ts.schema));

      result.put(ts, rows);
    }

    return result;
  }

  private List<TopicSchema> buildSchemas(String inputTopics) {
    return Arrays.stream(inputTopics.split(","))
        .map(PubsubUtils::getTopicSchema)
        .collect(Collectors.toList());
  }
}
