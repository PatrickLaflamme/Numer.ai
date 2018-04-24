package com.patrick.laflamme.numerpaipe;

import java.util.HashMap;
import java.util.Map;
import com.patrick.laflamme.numerpaipe.utils.WriteToText;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class epochScores {
    
    /**
   * Class to hold info about a game event.
   */
  @DefaultCoder(AvroCoder.class)
  static class tradeInfo {
    @Nullable String id;
    @Nullable String era;
    @Nullable String data_type;
    @Nullable Double[] features;
		@Nullable Integer target;

    public tradeInfo() {}

    public tradeInfo(String id, String era, String data_type, Double[] features, Integer target) {
      this.id = id;
      this.era = era;
      this.data_type = data_type;
      this.features = features;
      this.target = target;
    }

    public String getId() {
      return this.id;
    }
    public String getEra() {
      return this.era;
    }
    public String getDataType() {
      return this.data_type;
    }
    public Double[] getFeature(int index) {
      return this.features[index];
    }
    public Integer getTarget() {
      return this.target;
    }
    public String getKey(String keyname) {
      
      if(keyname.equals("data_type")){
        return this.data_type;
      }
      else if(keyname.equals("era")){
        return this.era;
      }
      else{
        //make the id the default
        return this.id;
      }
    }
  }

  static class ParseTradeFn extends DoFn<String, tradeInfo> {

      // Log and count parse errors.
      private static final Logger LOG = LoggerFactory.getLogger(ParseTradeFn.class);
      private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");
  
      @ProcessElement
      public void processElement(ProcessContext c) {
        String[] components = c.element().split(",");
        try {
          String id = components[0].trim();
          String era = components[1].trim();
          String data_type = components[2].trim();
          Double[] features = new Double[50];
          Integer target;


          for(int x=0; x<50; x++) {
            features[x] = Double.parseDouble(components[3+x].trim());
          }

          if(components.length == 54){
            target = Integer.parseInt(components[53].trim());
          } else {
            target = null;
          }

          tradeInfo tInfo = new tradeInfo(id, era, data_type, features, target);
          c.output(tInfo);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
          numParseErrors.inc();
          LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
        }
      }
    }

    protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>>
        configureOutput() {
      Map<String, WriteToText.FieldFn<KV<String, Integer>>> config = new HashMap<>();
      config.put("era", (c, w) -> c.element().getKey());
      config.put("mean_factor_score", (c, w) -> c.element().getValue());
      return config;
    }

    public static class ExtractAndMeanScore
        extends PTransform<PCollection<tradeInfo>, PCollection<KV<String, Double>>> {

      private final String field;
      private final Integer featureID;

      ExtractAndMeanScore(String field, Integer featureID) {
        this.field = field;
        this.featureID = featureID;
      }

      @Override
      public PCollection<KV<String, Integer>> expand(
          PCollection<tradeInfo> tradeMakeInfo) {

        return tradeMakeInfo
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                    .via((tradeInfo tInfo) -> KV.of(tInfo.getKey(field), tInfo.getFeature(featureID))))
            .apply(Mean.perKey());
      }
    }

    public interface Options extends PipelineOptions {

      @Description("Path to the data file(s) containing numer.ai data.")
      @Default.String("file://home/patricklaflamme/Desktop/numer.ai/Data/datasets/*/*_data.csv")
      String getInput();
      void setInput(String value);

      // Set this required option to specify where to write the output.
      @Description("Path of the file to write to.")
      @Validation.Required
      String getOutput();
      void setOutput(String value);
    }

  public static void main(String [] args) throws Exception{
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Read events from a text file and parse them.
    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseTradeEvent", ParDo.of(new ParseTradeFn()))
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractEraMeans", new ExtractAndMeanScore("era",1))
        .apply(
            "WriteEraMeans", new WriteToText<>(options.getOutput(), configureOutput(), false));

    // Run the batch pipeline.
    pipeline.run().waitUntilFinish();

  }
}