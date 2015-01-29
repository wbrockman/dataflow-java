/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.genomics.dataflow.utils;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.ReadGroupSet;
import com.google.api.services.genomics.model.SearchReadGroupSetsRequest;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.api.services.genomics.model.SearchVariantsRequest;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.genomics.utils.Contig;
import com.google.cloud.genomics.utils.GenomicsFactory;
import com.google.cloud.genomics.utils.Paginator;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.logging.Logger;

import static com.google.cloud.genomics.utils.Paginator.*;

/**
 * A common options class for all pipelines that operate over a single dataset and write their
 * analysis to a file.
 */
public interface GenomicsDatasetOptions extends GenomicsOptions {
  public static class Methods {

    private static final Logger LOG = Logger.getLogger(GenomicsDatasetOptions.class.getName());

    public static List<SearchReadsRequest> getReadRequests(GenomicsDatasetOptions options,
                                                           GenomicsFactory.OfflineAuth auth,
                                                           boolean excludeXY)
        throws IOException, GeneralSecurityException {
      Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

      // Figure out the datasetId and readGroupSetId(s) we are working with.
      String datasetId = options.getDatasetId();
      List<String> readGroupSetIds = Lists.newArrayList();
      String readGroupSetId = options.getReadGroupSetId();
      if (readGroupSetId.isEmpty()) {
        // If no particular readGroupSetId is specified, use everything in the dataset.
        Paginator.ReadGroupSets searchReadGroupSets = Paginator.ReadGroupSets.create(genomics);
        for (ReadGroupSet readGroupSet : searchReadGroupSets.search(
            new SearchReadGroupSetsRequest().setDatasetIds(Lists.newArrayList(datasetId)))) {
          readGroupSetIds.add(readGroupSet.getId());
        }
      } else {
        readGroupSetIds.add(readGroupSetId);
      }
      //TODO: for now we assume only one reference for all the readgroupsets, without checking.
      readGroupSetId = readGroupSetIds.get(0);
      Iterable<Contig> contigs = options.isAllContigs()
          ? Contig.lookupContigs(genomics, readGroupSetId, excludeXY)
          : Contig.parseContigsFromCommandLine(options.getReferences());

      List<SearchReadsRequest> requests = Lists.newArrayList();
      for (String rgsId : readGroupSetIds) {
        for (Contig contig : contigs) {
          for (Contig shard : contig.getShards()) {
            LOG.info("Adding request for " + rgsId
                + " with " + shard.referenceName + " " + shard.start + " to " + shard.end);
            requests.add(shard.getReadsRequest(rgsId));
          }
        }
      }
      return requests;
    }

    public static List<SearchVariantsRequest> getVariantRequests(GenomicsDatasetOptions options,
                                                                 GenomicsFactory.OfflineAuth auth, boolean excludeXY)
        throws IOException, GeneralSecurityException {
      String datasetId = options.getDatasetId();
      Genomics genomics = auth.getGenomics(auth.getDefaultFactory());

      Iterable<Contig> contigs = options.isAllContigs()
          ? Contig.getContigsInVariantSet(genomics, datasetId, excludeXY)
          : Contig.parseContigsFromCommandLine(options.getReferences());

      List<SearchVariantsRequest> requests = Lists.newArrayList();
      for (Contig contig : contigs) {
        for (Contig shard : contig.getShards()) {
          LOG.info("Adding request with " + shard.referenceName + " " + shard.start + " to "
              + shard.end);
          requests.add(shard.getVariantsRequest(datasetId));
        }
      }
      return requests;
    }
  }

  @Description("The ID of the Google Genomics dataset this pipeline is working with. "
      + "Defaults to 1000 Genomes.")
  @Default.String("10473108253681171589")
  String getDatasetId();

  @Description("The ID of the Google Genomics ReadGroupSet this pipeline is working with. "
      + "Default (empty) indicates all ReadGroupSets.")
  @Default.String("")
  String getReadGroupSetId();

  @Description("Path of the file to write to")
  String getOutput();

  @Description("By default, PCA will be run on BRCA1, pass this flag to run on all "
      + "non X and Y contigs present in the dataset")
  boolean isAllContigs();

  void setAllContigs(boolean allContigs);

  void setDatasetId(String datasetId);

  void setReadGroupSetId(String readGroupSetId);

  void setOutput(String output);

  @Description("Comma separated tuples of reference:start:end,... Defaults to " + Contig.BRCA1)
  @Default.String(Contig.BRCA1)
  String getReferences();

  void setReferences(String references);
}
