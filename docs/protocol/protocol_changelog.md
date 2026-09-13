# Protocol Changelog

## TaskMain-v1 → v1.1

1. Distinguished TaskMain-Oracle from Matched-O1.
2. Separated R_REQ_KV_TASK from R_REQ_KV_V1.
3. Set equivalent capacity to 585 pages/Pod.
4. Defined proactive q=0.05 as a byte token bucket.
5. Added the burst-cap coverage rule.
6. Named Recency q as selection quantile.
7. Added 60-second Recency decay.
8. Changed Recency history from policy hits to external demand.
9. Defined Persistence Top-K feasibility search.
10. Added V_ref/T_ref normalization to Cost-aware.
11. Removed global-optimum language for Oracle.
12. Formal experiments may not stop early based on headroom.

## TaskMain-v1.1 Step 2 closure

- Replaced the Development-observed burst cap with the protocol structural cap of
  one Pod cache capacity (`585 * 14 MiB`). The average q=0.05 refill rate is unchanged.
- Added explicit R_REQ_KV_TASK request-partition and ticket lifecycle counters.
