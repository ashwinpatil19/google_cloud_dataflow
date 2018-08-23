import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import com.google.cloud.bigquery.*;

import org.apache.kafka.common.protocol.types.Field;
import org.joda.time.Duration;
import org.omg.CORBA.StringHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import static com.google.common.base.Preconditions.checkArgument;

//Concepts:
// 1. Reading data from text files
// 2. Specifying 'inline' transforms
// 3. Counting items in a PCollection
// 4. Writing data to text files


public class MinimalWordCount {

    //project_name:apache_beam_etl.beam_etl
    //Target Table setup - 1
    private static final String BEAM_SAMPLES_TABLE = "project_name:apache_beam_etl.beam_etl";

    //Target Table setup - 2 - Delete later
    public static final String projectId = "project_name"; ///UpperCasing - final needs to be in upper case
    public static final String datasetName = "apache_beam_etl";  ///UpperCasing
    public static final String tableName = "beam_etl";  ///UpperCasing

    //Setup BigQuery
    public static BigQuery BIGQUERY = com.google.cloud.bigquery.BigQueryOptions.getDefaultInstance()
            .toBuilder()
            .setProjectId(projectId)
            .build()
            .getService();

    //Logger
    private static final Logger LOG = LoggerFactory.getLogger(MinimalWordCount.class);

    public static void main(String[] args) {

        System.out.println("bigquery: "+BIGQUERY.toString());


        //DirectRunner by default execution option
//        PipelineOptions options = PipelineOptionsFactory.create();
//        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        PipelineOptionsFactory.register(TemplateOptions.class);
        TemplateOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class);
//        GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
        System.out.println(options);

        options.setRunner(DataflowRunner.class);
        System.out.println("Using Runner" + options.getRunner() + "\n");
        options.setTempLocation("gs://project_name/apache_beam_etl");

        String tempLocation = options.getTempLocation();
        System.out.println("tempLocation: " + tempLocation);

        if (tempLocation.isEmpty())
        System.out.println("BigQueryIO.Write needs a GCS temp location to store temp files.");

        // Create the Pipeline object with the options we defined above
        Pipeline p = Pipeline.create(options);

//        project_name:apache_beam_etl.beam_etl
//        TableReference tableRef = new TableReference();
//        tableRef.setDatasetId("apache_beam_etl");
//        tableRef.setProjectId("magnetic-icon-167014");
//        tableRef.setTableId("beam_etl");
//
//        System.out.println("tableRef: "+ tableRef);

        // This example reads a public data set consisting of the complete works of Shakespeare.
        System.out.println("Reading shakespeare!!");

        //Root transform to the pipeline returns a PCollection where each element is one line
        PCollection<String> lines = p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"));

        //Apply a FlatMapElements transform the PCollection into individual word in Shakespeare's texts
        lines
             .apply(
                    FlatMapElements.into(TypeDescriptors.strings())
                            .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
                // We use a Filter transform to avoid empty word
                .apply(Filter.by((String word) -> !word.isEmpty()))
                .apply(Count.perElement())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(
                                        (KV<String, Long> wordCount) ->
                                                wordCount.getKey() + ": " + wordCount.getValue()))
//                .apply(TextIO.write().to("new_wordcounts"));
                .apply("TRANSFORM", ParDo.of(new TransformParDo()))
                .apply("WRITE", BigQueryIO.writeTableRows()
                        .to(String.format("project_name:apache_beam_etl.beam_etl",
                                options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND)
                        .withSchema(getTableSchema()));

        LOG.info("Metric Name: " + lines );
        System.out.println("print p: " +p);
        p.run().waitUntilFinish();
        System.out.println("Finished Writing TextFile!!");
    }

    public interface TemplateOptions extends DataflowPipelineOptions {
        @Description("GCS path of the file to read from")
        ValueProvider<String> getInputFile();

        void setInputFile(ValueProvider<String> value);
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("word").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    private static class TransformParDo extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String[] split = c.element().split(": ");
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchema().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
            System.out.println("printing row: " + row );
        }
    }

}
