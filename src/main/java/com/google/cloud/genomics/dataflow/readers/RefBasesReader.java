/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.genomics.dataflow.readers;

import com.google.api.services.genomics.Genomics;
import com.google.api.services.genomics.model.ListBasesResponse;
import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.model.ReferenceBases;
import com.google.cloud.genomics.dataflow.model.ReferenceInterval;
import com.google.cloud.genomics.utils.GenomicsFactory;

import java.io.IOException;
import java.util.logging.Logger;

public class RefBasesReader extends GenomicsApiReader<SearchReadsRequest,
        KV<ReferenceInterval, ReferenceBases>> {
  private static final Logger LOG = Logger.getLogger(RefBasesReader.class.getName());

  /**
   * Create a ReadReader with no fields parameter: all information will be returned.
   * @param auth Auth class containing credentials.
   */
  public RefBasesReader(GenomicsFactory.OfflineAuth auth) {
    super(auth, null);
  }

  @Override
  protected void processApiCall(Genomics genomics, ProcessContext c, SearchReadsRequest request)
      throws IOException {
    LOG.info("Reading Reference bases: id " + request.getReferenceName() + " start "
    + request.getStart() + " end " + request.getEnd());
    ReferenceInterval ref = ReferenceInterval.fromRequest(request);
    Genomics.References.Bases.List listRequest =
            genomics.references().bases().list(request.getReferenceName());
    listRequest.setStart(request.getStart());
    listRequest.setEnd(request.getEnd());
    final ListBasesResponse result = listRequest.execute();
    c.output(KV.of(ref, ReferenceBases.create(ref, result.getSequence())));
  }
}
