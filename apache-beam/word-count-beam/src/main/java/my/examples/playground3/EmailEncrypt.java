package my.examples.playground3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * To execute this pipeline, run the command below.
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass=my.examples.playground3.EmailEncrypt \
 * -Dexec.args="--inputFile=test2_email-encrypt.csv --output=enc --suffix=.csv" -Pdirect-runner
 * }</pre>
 */
public class EmailEncrypt {
  private static final Schema SCHEMA = Schema.of(
      Schema.Field.of("name", Schema.FieldType.STRING),
      Schema.Field.of("email", Schema.FieldType.STRING),
      Schema.Field.nullable("encryptedEmail", Schema.FieldType.STRING),
      Schema.Field.of("encKey", Schema.FieldType.STRING));

  public interface EmailEncryptOptions extends PipelineOptions {
    String getInputFile();
    void setInputFile(String value);
    String getOutput();
    void setOutput(String value);
    String getSuffix();
    void setSuffix(String value);
  }

  public static void main(String[] args) {
    EmailEncryptOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(EmailEncryptOptions.class);

    runEmailEncrypt(options);
  }

  private static void runEmailEncrypt(EmailEncryptOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<Row> rows = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply("ParseInput", new ParseInput()).setRowSchema(SCHEMA)
        .apply("EncryptEmail", new EncryptEmail()).setRowSchema(SCHEMA);

    rows.apply("FormatAsCsv", new FormatAsCsv())
        .apply("WriteLines",
            TextIO.write().to(options.getOutput()).withSuffix(options.getSuffix()).withoutSharding());

    rows.apply("FormatAsTxt", new FormatAsTxt())
        .apply("WriteLines",
            TextIO.write().to("enc-count").withSuffix(".txt").withoutSharding());

    p.run().waitUntilFinish();
  }

  private static class ParseInput extends PTransform<PCollection<String>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollection<String> input) {
      return input.apply(MapElements.into(TypeDescriptors.rows())
          .via(element -> {
            String[] data = element.split(",", -1);

            return Row.withSchema(SCHEMA)
                .withFieldValue("name", data[0].trim())
                .withFieldValue("email", data[1].trim())
                .withFieldValue("encKey", data[2].trim())
                .build();
          })
      );
    }
  }

  private static class EncryptEmail extends PTransform<PCollection<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      return input.apply(ParDo.of(new DoFn<Row, Row>() {
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
              .withFieldValue("encryptedEmail", encryptedEmail).build());
        }
      }));
    }
  }

  private static class FormatAsCsv extends PTransform<PCollection<Row>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<Row> input) {
      return input.apply(Filter.by(v -> v.getString("encryptedEmail") != null))
          .apply(MapElements.via(new SimpleFunction<Row, String>() {
            @Override
            public String apply(Row input) {
              return input.getString("name") + ","
                  + input.getString("encryptedEmail") + ","
                  + input.getString("encKey");
            }
          }));
    }
  }

  private static class FormatAsTxt extends PTransform<PCollection<Row>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<Row> input) {
      return input.apply(ParDo.of(new DoFn<Row, Boolean>() {
            @ProcessElement
            public void processElement(@Element Row element, OutputReceiver<Boolean> receiver) {
              receiver.output(element.getString("encryptedEmail") != null);
            }
          }))
          .apply(Count.perElement())
          .apply(MapElements.via(new SimpleFunction<KV<Boolean, Long>, String>() {
            @Override
            public String apply(KV<Boolean, Long> input) {
              return (input.getKey() ? "Encrypted: " : "Failed: ") + input.getValue();
            }
          }));
    }
  }
}
