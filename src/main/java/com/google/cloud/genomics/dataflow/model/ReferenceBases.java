package com.google.cloud.genomics.dataflow.model;

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.DelegatingAtomicCoder;

/**
 * Created by brockman on 2/3/15.
 */
@DefaultCoder(ReferenceBases.Coder.class)
public class ReferenceBases {
    public static class Coder
            extends DelegatingAtomicCoder<ReferenceBases,
            KV<ReferenceInterval, String>> {

        private static final Coder INSTANCE = new Coder();

        public static Coder of() {
            return INSTANCE;
        }

        private Coder() {
            super(KvCoder.of(
                    ReferenceInterval.Coder.of(),
                    StringUtf8Coder.of()));
        }

        @Override protected ReferenceBases from(KV<ReferenceInterval, String> kv) {
            return ReferenceBases.create(kv.getKey(), kv.getValue());
        }

        @Override protected KV<ReferenceInterval, String> to(ReferenceBases bases) {
            return KV.of(bases.getInterval(), bases.getSequence());
        }
    }

    public ReferenceInterval getInterval() {
        return interval;
    }

    private final ReferenceInterval interval;
    protected final String sequence;

    public String getId() {
        return interval.getId();
    }

    public Long getStart() {
        return interval.getStart();
    }

    public Long getEnd() {
        return interval.getEnd();
    }

    public String getSequence() {
        return sequence;
    }

    protected ReferenceBases(ReferenceInterval interval, String sequence) {
        this.interval = interval;
        this.sequence = sequence;
    }

    public static ReferenceBases create(ReferenceInterval interval, String sequence) {
        return new ReferenceBases(interval, sequence);
    }

    public String getSequence(Long start, Long end) {
        return sequence.substring(
                interval.clippedRelativePosition(start).intValue(),
                interval.clippedRelativePosition(end).intValue());
    }
}
