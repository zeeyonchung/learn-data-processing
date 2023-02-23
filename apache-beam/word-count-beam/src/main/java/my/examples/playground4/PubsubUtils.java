package my.examples.playground4;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;

public class PubsubUtils {
  public static class TopicSchema {
    final String topic;
    final String inputFilePath;
    final String outputFileName;
    final Schema schema;

    public TopicSchema(String topic, String inputFilePath, String outputFileName, Schema schema) {
      this.topic = topic;
      this.inputFilePath = inputFilePath;
      this.outputFileName = outputFileName;
      this.schema = schema;
    }
  }

  private static final TopicSchema USER_TOPIC_SCHEMA = new TopicSchema(
      "user",
      "test4_user.csv",
      "test4_output",
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("email", Schema.FieldType.STRING))
  );

  private static final List<TopicSchema> TOPIC_SCHEMAS = Collections
      .singletonList(USER_TOPIC_SCHEMA);

  public static TopicSchema getTopicSchema(String topic) {
    return TOPIC_SCHEMAS.stream()
        .filter(ts -> topic.equals(ts.topic))
        .findFirst()
        .orElseThrow(IllegalArgumentException::new);
  }
}
