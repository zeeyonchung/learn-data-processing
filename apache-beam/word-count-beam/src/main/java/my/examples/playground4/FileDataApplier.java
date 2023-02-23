package my.examples.playground4;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class FileDataApplier extends PTransform<PCollection<Row>, PDone> {
  private final String outputFileName;

  private FileDataApplier(String outputFileName) {
    this.outputFileName = outputFileName;
  }

  public static FileDataApplier of(String outputFileName) {
    return new FileDataApplier(outputFileName);
  }

  @Override
  public PDone expand(PCollection<Row> input) {
    return input
        .apply(MapElements.via(new SimpleFunction<Row, String>() {
          @Override
          public String apply(Row input) {
            return input.getString("name") + ","
                + input.getString("email");

          }
        }))
        .apply("WriteLines",
            TextIO.write().to(outputFileName).withSuffix(".csv").withoutSharding());
  }
}
