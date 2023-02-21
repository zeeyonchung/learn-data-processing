package my.examples.playground1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * To execute this pipeline, run the command below.
 * <pre>{@code
 * mvn compile exec:java -Dexec.mainClass=my.examples.playground1.EmailEncrypt \
 * -Dexec.args="--inputFile=test_email-encrypt.csv --output=enc --suffix=.csv" -Pdirect-runner
 * }</pre>
 */
public class EmailEncrypt {
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

    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply("ParseUserInfo", ParDo.of(new ParseUserInfoFn()))
            .apply("EncryptEmail", ParDo.of(new EncryptEmail()))
            .apply("FormatAsCSV", MapElements.via(new FormatAsCsvFn()))
            .apply("WriteLines", TextIO.write().to(options.getOutput()).withSuffix(options.getSuffix()));

    p.run().waitUntilFinish();
  }

  private static class ParseUserInfoFn extends DoFn<String, UserInfo> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<UserInfo> receiver) {
      String[] data = element.split(",", -1);
      UserInfo user = new UserInfo(data[0].trim(), data[1].trim());
      receiver.output(user);
    }
  }

  private static class EncryptEmail extends DoFn<UserInfo, UserInfo> {
    @ProcessElement
    public void processElement(@Element UserInfo element, OutputReceiver<UserInfo> receiver) {
      receiver.output(element.encryptEmail());
    }
  }

  private static class FormatAsCsvFn extends SimpleFunction<UserInfo, String> {
    @Override
    public String apply(UserInfo input) {
      return input.toCSV();
    }
  }
}
