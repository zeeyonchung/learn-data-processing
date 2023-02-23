package my.examples.playground4;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ParseRows extends PTransform<PCollection<String>, PCollection<Row>> {
  private final Schema schema;

  public ParseRows(Schema schema) {
    this.schema = schema;
  }

  public static ParseRows withSchema(Schema schema) {
    return new ParseRows(schema);
  }

  @Override
  public PCollection<Row> expand(PCollection<String> input) {
    return input
        .apply(MapElements.into(TypeDescriptors.rows())
            .via(element -> {
              String[] data = element.split(",", -1);

              return Row.withSchema(schema)
                  .withFieldValue("name", data[0].trim())
                  .withFieldValue("email", data[1].trim())
                  .build();
            })
        );
  }
}
