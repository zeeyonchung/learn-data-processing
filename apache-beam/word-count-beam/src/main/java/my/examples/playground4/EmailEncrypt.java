package my.examples.playground4;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class EmailEncrypt extends PTransform<PCollection<Row>, PCollection<Row>> {
  private EmailEncrypt() {}

  public static EmailEncrypt of() {
    return new EmailEncrypt();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(ParDo.of(new DoFn<Row, Row>() {
          @ProcessElement
          public void processElement(@Element Row element, OutputReceiver<Row> receiver) {
            String encryptedEmail = "encrypted-" + element.getString("email");

            receiver.output(Row.fromRow(element)
                .withFieldValue("email", encryptedEmail)
                .build());
          }
        }));
  }
}
