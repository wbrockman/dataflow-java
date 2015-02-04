package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.CigarUnit;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.genomics.dataflow.model.ReferenceBases;
import com.google.cloud.genomics.dataflow.model.ReferenceInterval;

/**
 * Created by brockman on 1/29/15.
 */
public class ReadPrinter extends com.google.cloud.dataflow.sdk.transforms.DoFn<KV<ReferenceInterval,
        CoGbkResult>, String> {
  private final TupleTag<ReferenceBases> refTag;
  private final TupleTag<Read> readTag;

  public ReadPrinter(TupleTag<ReferenceBases> refTag, TupleTag<Read> readTag) {
    this.refTag = refTag;
    this.readTag = readTag;
  }

  @Override
  public void processElement(ProcessContext processContext) throws Exception {
    KV<ReferenceInterval, CoGbkResult> input = processContext.element();
    ReferenceInterval interval = input.getKey();
    ReferenceBases refBases = input.getValue().getOnly(refTag);
    Iterable<Read> reads = input.getValue().getAll(readTag);
    for (Read read : reads) {
      StringBuilder out = new StringBuilder(read.getAlignedSequence().length());
      Long offset = (long) 0;
      for (CigarUnit c : read.getAlignment().getCigar()) {
        switch (c.getOperation()) {
          case "ALIGNMENT_MATCH":
          case "SEQUENCE_MATCH":
          case "SEQUENCE_MISMATCH":
            int start = offset.intValue();
            offset += c.getOperationLength();
            out.append(read.getAlignedSequence().substring(start, offset.intValue()));
            break;
          case "CLIP_SOFT":
          case "INSERT":
            offset += c.getOperationLength();
            break;
          case "PAD":
            repeat(out, '*', c.getOperationLength());
            break;
          case "DELETE":
            repeat(out, '-', c.getOperationLength());
            break;
          case "SKIP":
            repeat(out, ' ', c.getOperationLength());
            break;
          case "CLIP_HARD":
            break;
        }
      }
      processContext.output(out.toString());
      processContext.output(refBases.getSequence(
              read.getAlignment().getPosition().getPosition(),
              read.getAlignment().getPosition().getPosition() + read.getAlignedSequence().length()));
    }
  }

  private void repeat(StringBuilder out, char symbol, long copies) {
    for (int i = 0; i < copies; i++) {
      out.append(symbol);
    }
  }
}
