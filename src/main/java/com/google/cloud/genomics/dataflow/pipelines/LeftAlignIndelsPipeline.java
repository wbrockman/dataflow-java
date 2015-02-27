package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.Read;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.functions.ReadPrinter;
import com.google.cloud.genomics.dataflow.model.ReferenceBases;
import com.google.cloud.genomics.dataflow.model.ReferenceInterval;
import com.google.cloud.genomics.dataflow.readers.ReadReader;
import com.google.cloud.genomics.dataflow.readers.RefBasesReader;
import com.google.cloud.genomics.dataflow.utils.DataflowWorkarounds;
import com.google.cloud.genomics.dataflow.utils.GenomicsDatasetOptions;
import com.google.cloud.genomics.dataflow.utils.GenomicsOptions;
import com.google.cloud.genomics.utils.Contig;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import static com.google.cloud.genomics.utils.GenomicsFactory.OfflineAuth;

/**
 * Created by brockman on 1/28/15.
 */
public class LeftAlignIndelsPipeline {
  public static void main(String[] args) throws GeneralSecurityException, IOException {
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    GenomicsOptions.Methods.validateOptions(options);

    OfflineAuth auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    Genomics genomics = auth.getGenomics(auth.getDefaultFactory());
    List<String> readGroupSetIds = GenomicsDatasetOptions.Methods.getReadGroupSetIds(options, genomics);

    Iterable<Contig> contigs = GenomicsDatasetOptions.Methods.getShardedContigs(options, genomics, readGroupSetIds);

    List<SearchReadsRequest> readRequests =
        GenomicsDatasetOptions.Methods.getSearchReadsRequests(readGroupSetIds, contigs);

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    PCollection<SearchReadsRequest> readRequestCollection = DataflowWorkarounds.getPCollection(
        readRequests, p, options.getNumWorkers());
    PCollection<KV<ReferenceInterval, Read>> reads = readRequestCollection.apply(
            ParDo.of(new ReadReader(auth)).named(ReadReader.class.getSimpleName()));
    PCollection<KV<ReferenceInterval, ReferenceBases>> refs = refRequests.apply(
            ParDo.of(new RefBasesReader(auth)).named(RefBasesReader.class.getSimpleName()));
    final TupleTag<ReferenceBases> refTag = new TupleTag<>("refTag");
    final TupleTag<Read> readTag = new TupleTag<>("readTag");
    PCollection<KV<ReferenceInterval, CoGbkResult>> refAndReads =
            KeyedPCollectionTuple.of(refTag, refs)
            .and(readTag, reads)
            .apply(CoGroupByKey.<ReferenceInterval>create());
    //TODO: insert an actual LeftAlignIndels operation before ReadPrinter
    PCollection<String> printed = refAndReads.apply(
            ParDo.of(new ReadPrinter(refTag, readTag)).named(ReadPrinter.class.getSimpleName()));
    printed.apply(TextIO.Write.to(options.getOutput()).named("WriteReads"));

    p.run();
  }

}
