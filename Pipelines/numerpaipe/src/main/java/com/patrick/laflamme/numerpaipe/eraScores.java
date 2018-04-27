package com.patrick.laflamme.numerpaipe;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple.TaggedKeyedPCollection;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class eraScores {
    
    /**
   * Class to hold info about a trade event.
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
    public Double getFeature(int index) {
      return this.features[index];
    }
    public Double[] getAllFeatures() {
      return this.features;
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

  public static class ExtractAndMeanScore
      extends PTransform<PCollection<tradeInfo>, PCollection<KV<String, Double>>> {

    private final String field;
    private final Integer featureID;

    ExtractAndMeanScore(String field, Integer featureID) {
      this.field = field;
      this.featureID = featureID;
    }

    @Override
    public PCollection<KV<String, Double>> expand(
        PCollection<tradeInfo> tradeMakeInfo) {

      return tradeMakeInfo
          .apply(
              MapElements.into(
                      TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                  .via((tradeInfo tInfo) -> KV.of(tInfo.getKey(field), tInfo.getFeature(featureID))))
          .apply(Mean.perKey());
    }
  }

  static class MeanFeatures extends PTransform<PCollection<tradeInfo>, PCollection<KV<String,Double[]>>>{

    private final String field;

    MeanFeatures(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Double[]>> expand(
      PCollection<tradeInfo> tradeMakeInfo) {

      Integer nFeatures = 50;
      
      PCollection<KV<String, Double>> feature;
      Pipeline pipe = tradeMakeInfo.getPipeline();
      KeyedPCollectionTuple<String> features = KeyedPCollectionTuple.empty(pipe);
      List<TupleTag<Double>> tags = new ArrayList<TupleTag<Double>>(0);

      PCollectionList<KV<String, Double>> pcs;

      for(int i = 0; i < nFeatures; i++){
        TupleTag<Double> currentTag = new TupleTag<Double>(Integer.toString(i));
        tags.add(currentTag);

        feature = tradeMakeInfo.apply(new ExtractAndMeanScore(field, i));
        features = features.and(currentTag, feature);
      }

      PCollection<KV<String, CoGbkResult>> keyValues = features.apply(CoGroupByKey.create());

      return keyValues.apply(ParDo.of(new FlattenCoGbkResult(tags)));
    }

  }


  static class FlattenCoGbkResult extends DoFn<KV<String, CoGbkResult>, KV<String, Double[]>>{
    
    private final List<TupleTag<Double>> tags;

    FlattenCoGbkResult(List<TupleTag<Double>> tags){
      this.tags = tags;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      
      KV<String, CoGbkResult> e = c.element();

      Integer nFeatures = tags.size();

      Double[] featureArray = new Double[nFeatures];

      for(int i = 0; i < nFeatures; i++){

        TupleTag<Double> tag = tags.get(i);
        Double feature = e.getValue().getOnly(tag);

        featureArray[i] = feature;
      }
      String key = e.getKey();

      c.output(KV.of(key, featureArray));
    }
  }

  static class StringifyGroups extends DoFn<KV<String, Double[]>, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {

      String groupID = c.element().getKey();
      Double[] features = c.element().getValue();
      Integer nFeatures = features.length;
      List<String> outputList = new ArrayList<String>(51);

      outputList.add(0, groupID);

      for(int i = 1; i <= nFeatures; i++){
        outputList.add(i, Double.toString(features[i-1]));
      }

      c.output(String.join(",", outputList));
    }
  }

  protected static Map<String, WriteToText.FieldFn<KV<String, Double[]>>>
      configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Double[]>>> config = new HashMap<>();
    config.put("era", (c, w) -> c.element().getKey());
    config.put("mean_factor_scores", (c, w) -> c.element().getValue());
    return config;
  }

  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing numer.ai data.")
    @Default.String("file://home/patricklaflamme/Desktop/numer.ai/Data/datasets/numerai_dataset_104/*_data.csv")
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
    pipeline.apply(TextIO.read().from(options.getInput()))
            .apply("ParseTradeEvent", ParDo.of(new ParseTradeFn()))
            .apply("ExtractEraMeans", new MeanFeatures("era"))
            .apply("StringifyEraMeans", ParDo.of(new StringifyGroups()))
            .apply("WriteEraMeans", TextIO.write().to(options.getOutput()));

    // Run the batch pipeline.
    pipeline.run().waitUntilFinish();

  }
}