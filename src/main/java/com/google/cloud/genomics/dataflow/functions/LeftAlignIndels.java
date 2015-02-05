package com.google.cloud.genomics.dataflow.functions;

import com.google.api.services.genomics.model.Read;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
/*import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMRecord;
*/

/**
 * Created by brockman on 1/30/15.
 */
public class LeftAlignIndels extends DoFn<Read, Read> {

  @Override
  public void processElement(ProcessContext processContext) throws Exception {

  }
/*
  public void apply(SAMRecord read, ReferenceContext ref) {
    // we can not deal with screwy records
    if ( read.getReadUnmappedFlag() || read.getCigar().numCigarElements() == 0 ) {
      outputWriter.addAlignment(read);
      return;
    }

    // move existing indels (for 1 indel reads only) to leftmost position within identical sequence
    int numBlocks = AlignmentUtils.getNumAlignmentBlocks(read);
    if ( numBlocks == 2 ) {
      Cigar newCigar = AlignmentUtils.leftAlignIndel(CigarUtils.unclipCigar(read.getCigar()), ref.getBases(), read.getReadBases(), 0, 0, true);
      newCigar = CigarUtils.reclipCigar(newCigar, read);
      read.setCigar(newCigar);
    }

    outputWriter.addAlignment(read);
  }

  private class ReferenceContext {
  }
*/
}
