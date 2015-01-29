package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.CigarUnit;
import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.List;

/**
 * Created by brockman on 1/29/15.
 */
public class ReadPrinter extends com.google.cloud.dataflow.sdk.transforms.DoFn<Read, String> {

  @Override
  public void processElement(ProcessContext processContext) throws Exception {
    Read read = processContext.element();
    StringBuilder out = new StringBuilder();
    Long offset = Long.valueOf(0);
    for (CigarUnit c :read.getAlignment().getCigar()) {
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
  }

  private void repeat(StringBuilder out, char symbol, long copies) {
    for (int i = 0; i < copies; i++) {
      out.append(symbol);
    }
  }
}
