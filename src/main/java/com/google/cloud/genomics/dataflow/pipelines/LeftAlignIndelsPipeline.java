package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.genomics.dataflow.functions.LeftAlignIndels;
import com.google.cloud.genomics.dataflow.functions.ReadPrinter;
import com.google.cloud.genomics.dataflow.readers.ReadReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import static com.google.cloud.genomics.utils.GenomicsFactory.*;

/**
 * Created by brockman on 1/28/15.
 */
public class LeftAlignIndelsPipeline {
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    GenomicsOptions.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    List<SearchReadsRequest> requests = GenomicsDatasetOptions.Methods.getReadRequests(
        options, auth, true);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    PCollection<SearchReadsRequest> readRequests = DataflowWorkarounds.getPCollection(
        requests, p, options.getNumWorkers());
    PCollection<Read> reads = readRequests.apply(
        ParDo.of(new ReadReader(auth)).named(ReadReader.class.getSimpleName()));
    //TODO: insert an actual LeftAlignIndels operation before ReadPrinter
    PCollection<Read> leftAlignedReads = reads.apply(
        ParDo.of(new LeftAlignIndels()).named(LeftAlignIndels.class.getSimpleName()));
    PCollection<String> printed = reads.apply(ParDo.of(new ReadPrinter()).named(ReadPrinter.class.getSimpleName()));
    printed.apply(TextIO.Write.to(options.getOutput()).named("WriteReads"));

    p.run();
  }

}
