package my.examples.playground2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * To execute this pipeline, run the command below.
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass=my.examples.playground2.EmailEncrypt \
 * -Dexec.args="--inputFile=test2_email-encrypt.csv --output=enc --suffix=.csv" -Pdirect-runner
 * }</pre>
 */
public class EmailEncrypt {
  private static final Schema SCHEMA = Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("email", Schema.FieldType.STRING),
          Schema.Field.nullable("encryptedEmail", Schema.FieldType.STRING),
          Schema.Field.of("encKey", Schema.FieldType.STRING));

  public static void main(String[] args) {
    EmailEncryptOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(EmailEncryptOptions.class);

    runEmailEncrypt(options);
  }

  public interface EmailEncryptOptions extends PipelineOptions {
    String getInputFile();
    void setInputFile(String value);
    String getOutput();
    void setOutput(String value);
    String getSuffix();
    void setSuffix(String value);
  }

  private static void runEmailEncrypt(EmailEncryptOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<Row> rows = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply("ParseInput", ParDo.of(new ParseInput()))
            .setRowSchema(SCHEMA)
            .apply("EncryptEmail", ParDo.of(new EncryptEmail()))
            .setRowSchema(SCHEMA);

    rows.apply("FilterSuccess", Filter.by(v -> v.getString("encryptedEmail") != null))
            .apply("FormatAsCsv", MapElements.via(new FormatAsCsvFn()))
            .apply("WriteLines", TextIO.write().to(options.getOutput())
                    .withSuffix(options.getSuffix()).withoutSharding());

    rows.apply("CheckSuccess", ParDo.of(new CheckSuccess()))
            .apply("CountSuccess", Count.perElement())
            .apply("FormatAsTxt", MapElements.via(new FormatAsTxtFn()))
            .apply("WriteLines", TextIO.write().to("enc-count")
                    .withSuffix(".txt").withoutSharding());

    p.run().waitUntilFinish();
  }

  private static class ParseInput extends DoFn<String, Row> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<Row> receiver) {
      String[] data = element.split(",", -1);

      Row row = Row.withSchema(SCHEMA)
              .withFieldValue("name", data[0].trim())
              .withFieldValue("email", data[1].trim())
              .withFieldValue("encKey", data[2].trim())
              .build();

      receiver.output(row);
    }
  }

  private static class EncryptEmail extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<Row> receiver) {
      String encryptedEmail = null;

      try {
        encryptedEmail = AES256.encrypt(
                element.getString("encKey"),
                element.getString("email"));
      } catch (Exception e) {
        System.out.println("Encryption failed.");
      }

      receiver.output(Row.fromRow(element)
              .withFieldValue("encryptedEmail", encryptedEmail)
              .build());
    }
  }

  private static class CheckSuccess extends DoFn<Row, Boolean> {
    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<Boolean> receiver) {
      receiver.output(element.getString("encryptedEmail") != null);
    }
  }

  private static class FormatAsCsvFn extends SimpleFunction<Row, String> {
    @Override
    public String apply(Row input) {
      return input.getString("name") + "," +
              input.getString("encryptedEmail") + "," +
              input.getString("encKey");
    }
  }

  private static class FormatAsTxtFn extends SimpleFunction<KV<Boolean, Long>, String> {
    @Override
    public String apply(KV<Boolean, Long> input) {
      String title = input.getKey() ? "Encrypted" : "Failed";
      return title + ": " + input.getValue();
    }
  }
}
