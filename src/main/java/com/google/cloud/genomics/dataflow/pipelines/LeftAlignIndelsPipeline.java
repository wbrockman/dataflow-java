package com.google.cloud.genomics.dataflow.pipelines;

import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
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
  private static final String READ_FIELDS = "nextPageToken";

  public static int main(String[] args) {
    GenomicsDatasetOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(GenomicsDatasetOptions.class);
    GenomicsOptions.Methods.validateOptions(options);

    OfflineAuth auth;
    try {
      auth = GenomicsOptions.Methods.getGenomicsAuth(options);
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    } catch (GeneralSecurityException e) {
      e.printStackTrace();
      return 1;
    }
    List<SearchReadsRequest> requests;
    try {
      requests = GenomicsDatasetOptions.Methods.getReadRequests(
          options, auth, true);
    } catch (IOException e) {
      e.printStackTrace();
      return 2;
    } catch (GeneralSecurityException e) {
      e.printStackTrace();
      return 2;
    }

    Pipeline p = Pipeline.create(options);
    DataflowWorkarounds.registerGenomicsCoders(p);
    DataflowWorkarounds.getPCollection(requests, p, options.getNumWorkers());

    return 0;
  }

}
