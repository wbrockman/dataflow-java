package com.google.cloud.genomics.dataflow.model;

import com.google.api.services.genomics.model.SearchReadsRequest;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.genomics.dataflow.coders.DelegatingAtomicCoder;

/**
 * Created by brockman on 2/3/15.
 */
@DefaultCoder(ReferenceInterval.Coder.class)
public class ReferenceInterval {
    public static class Coder extends DelegatingAtomicCoder<ReferenceInterval,
                            KV<String, KV<Long, Long>>> {

        private static final Coder INSTANCE = new Coder();

        public static Coder of() {
            return INSTANCE;
        }

        public static <T> Coder of(Class<T> type) {
            return INSTANCE;
        }

        private Coder() {
            super(KvCoder.of(
                    StringUtf8Coder.of(), KvCoder.of(
                            BigEndianLongCoder.of(),
                            BigEndianLongCoder.of())));
        }

        @Override protected ReferenceInterval from(KV<String, KV<Long, Long>> kv1) {
            KV<Long, Long> kv2 = kv1.getValue();
            return ReferenceInterval.of(kv1.getKey(), kv2.getKey(), kv2.getValue());
        }

        @Override protected KV<String, KV<Long, Long>> to(ReferenceInterval interval) {
            return KV.of(
                    interval.getId(), KV.of(interval.getStart(), interval.getEnd()));
        }
    }

    public String getId() {
        return id;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Long clip(Long position) {
        return Long.min(Long.max(start, position), end);
    }

    public Long clippedRelativePosition(Long position) {
        return clip(position) - start;
    }

    private final String id;
    private final Long start;
    private final Long end;

    protected ReferenceInterval(String id, Long start, Long end) {
        this.id = id;
        this.start = start;
        this.end = end;
    }
    
    public static ReferenceInterval of(String id, Long start, Long end) {
        return new ReferenceInterval(id, start, end);
    }

    public static ReferenceInterval fromRequest(SearchReadsRequest request) {
        return new ReferenceInterval(request.getReferenceName(),
                request.getStart(),
                request.getEnd());
    }
}
