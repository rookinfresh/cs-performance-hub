#!/usr/bin/env python3
"""
Recompute historical UFR/renewal snapshot columns using current procedure definitions.

Fixes trend discontinuities caused by cohort definition changes (e.g. CORE_CONTRACTS
validation, prior-month existence check added to REFRESH procedure after initial backfill).

Updates per snapshot row:
  RENEWAL_M1/M2/M3: _PCT, _CALLED, _TOTAL, _M2M, _M2M_CALLED
  RENEWAL_COVERAGE_PCT (always M+2)
  COMPOSITE_SCORE (recomputed with new UFR + existing AR/call/SLA from snapshot)
  OVERALL_STATUS (recomputed from new composite)

Methodology:
  - Uses EXACT same renewal_cohort_all definition as current REFRESH procedure
    (CORE_CONTRACTS validation, MRR>0, 120-day outreach window, >=15min calls)
  - Outreach events capped at snapshot_date (no future leakage)
  - Composite reuses snapshot's existing ARS_PER_DAY, AVG_CALL_SCORE, RET_SLA_PCT

Known limitations:
  - CORE_CONTRACTS reflects current state, not historical
  - MRR projections may differ slightly from snapshot-time values
  - These trade-offs are intentional: consistent definitions > perfect reconstruction
"""

import snowflake.connector
from datetime import date, timedelta

START_DATE = date(2026, 1, 28)
END_DATE = date(2026, 3, 26)

UFR_RECOMPUTE_SQL = """
WITH csm_allowlist AS (
    SELECT DISTINCT SALESFORCE_USER_FULL_NAME AS CSM_NAME
    FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_IS_ACTIVE = TRUE
      AND (SALESFORCE_USER_TITLE ILIKE '%%customer success manager%%'
           OR SALESFORCE_USER_TITLE ILIKE 'csm%%'
           OR SALESFORCE_USER_TITLE = 'CSM')
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'Manager%%'
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'VP%%'
      AND SALESFORCE_USER_FULL_NAME NOT IN ('Maria Lam')
      AND (USER_WORKING_LOCATION IS NULL OR USER_WORKING_LOCATION NOT ILIKE '%%australia%%')
    UNION ALL
    SELECT SALESFORCE_USER_FULL_NAME FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_FULL_NAME IN ('Chris Isham') AND SALESFORCE_USER_IS_ACTIVE = TRUE
),

-- ── Base outreach events (wide window, capped at anchor) ──
sfdc_base AS (
    SELECT DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) AS EVENT_DATE,
           csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME,
           csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa
    INNER JOIN csm_allowlist al ON csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME = al.CSM_NAME
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG'
      AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed'
      AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) >= DATEADD('day', -120, DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)))
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) < '{anchor}'::DATE
),
gong_base AS (
    SELECT DATE(gc.CALL_EFFECTIVE_START_AT) AS EVENT_DATE,
           gc.CALL_SALESFORCE_USER_FULL_NAME AS CSM_NAME,
           gc.ORGANIZATION_UID, gc.CALL_ELAPSED_MINUTES
    FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN csm_allowlist al ON gc.CALL_SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.ORGANIZATION_UID IS NOT NULL
      AND gc.CALL_EFFECTIVE_START_AT >= DATEADD('day', -120, DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)))
      AND gc.CALL_EFFECTIVE_START_AT < '{anchor}'::DATE
),
podium_base AS (
    SELECT DATE(pc.CALL_INSERTED_AT) AS EVENT_DATE,
           pc.CALL_MADE_BY_USER_FULL_NAME AS CSM_NAME,
           pc.CALL_MADE_TO_ORGANIZATION_UID AS ORGANIZATION_UID, pc.CALL_DURATION_SECONDS
    FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN csm_allowlist al ON pc.CALL_MADE_BY_USER_FULL_NAME = al.CSM_NAME
    WHERE pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND pc.CALL_INSERTED_AT >= DATEADD('day', -120, DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)))
      AND pc.CALL_INSERTED_AT < '{anchor}'::DATE
),

-- ── Renewal cohort (current procedure definition) ──
renewal_cohort_all AS (
    SELECT DISTINCT u.SALESFORCE_USER_FULL_NAME AS CSM_NAME, m.ORGANIZATION_UID,
           DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) AS RENEWAL_MONTH,
           m.MONTH_START_CONTRACT_TYPE_BUCKET AS CONTRACT_TYPE
    FROM ANALYSIS.FINANCE.SALESFORCE_ORGANIZATION_MONTH_MRR_PLUS_6_MONTHS m
    INNER JOIN BUILD.SALESFORCE.CORE_ACCOUNTS a ON m.ORGANIZATION_UID = a.ORGANIZATION_UID
    INNER JOIN BUILD.SALESFORCE.CORE_USERS u ON a.ACCOUNT_OWNER_ID = u.SALESFORCE_USER_ID
    INNER JOIN csm_allowlist al ON u.SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    INNER JOIN BUILD.SALESFORCE.CORE_CONTRACTS c
        ON m.ORGANIZATION_UID = c.ORGANIZATION_UID
        AND c.CONTRACT_STATUS = 'Activated'
        AND c.CONTRACT_END_DATE >= DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH)
        AND c.CONTRACT_END_DATE < DATEADD('month', 1, DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH))
    WHERE m.ORGANIZATION_MONTH_START_MRR_USD > 0
      AND (
          (DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) = DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE))
           AND m.MONTH = DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)))
          OR
          (DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) = DATE_TRUNC('month', DATEADD('month', 2, '{anchor}'::DATE))
           AND m.MONTH = DATE_TRUNC('month', DATEADD('month', 2, '{anchor}'::DATE)))
          OR
          (DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) = DATE_TRUNC('month', DATEADD('month', 3, '{anchor}'::DATE))
           AND m.MONTH = DATE_TRUNC('month', DATEADD('month', 3, '{anchor}'::DATE)))
      )
),

-- ── Outreach joined to cohort (120-day window, capped at anchor) ──
renewal_ar_events AS (
    SELECT sb.EVENT_DATE, sb.CSM_NAME, sb.ORGANIZATION_UID FROM sfdc_base sb
    INNER JOIN renewal_cohort_all rc ON sb.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE sb.EVENT_DATE >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND sb.EVENT_DATE < LEAST(rc.RENEWAL_MONTH, '{anchor}'::DATE)
    UNION ALL
    SELECT gb.EVENT_DATE, gb.CSM_NAME, gb.ORGANIZATION_UID FROM gong_base gb
    INNER JOIN renewal_cohort_all rc ON gb.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE gb.CALL_ELAPSED_MINUTES >= 15
      AND gb.EVENT_DATE >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND gb.EVENT_DATE < LEAST(rc.RENEWAL_MONTH, '{anchor}'::DATE)
    UNION ALL
    SELECT pb.EVENT_DATE, pb.CSM_NAME, pb.ORGANIZATION_UID FROM podium_base pb
    INNER JOIN renewal_cohort_all rc ON pb.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE pb.CALL_DURATION_SECONDS >= 900
      AND pb.EVENT_DATE >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND pb.EVENT_DATE < LEAST(rc.RENEWAL_MONTH, '{anchor}'::DATE)
),

-- ── Coverage per CSM per renewal month ──
renewal_coverage_all AS (
    SELECT rc.CSM_NAME, rc.RENEWAL_MONTH,
           COUNT(DISTINCT rc.ORGANIZATION_UID) AS COHORT_TOTAL,
           COUNT(DISTINCT ar.ORGANIZATION_UID) AS COHORT_CALLED,
           ROUND(COUNT(DISTINCT ar.ORGANIZATION_UID)::FLOAT / NULLIF(COUNT(DISTINCT rc.ORGANIZATION_UID), 0), 3) AS COVERAGE_PCT,
           COUNT(DISTINCT CASE WHEN rc.CONTRACT_TYPE = 'Month-to-Month' THEN rc.ORGANIZATION_UID END) AS M2M_ORGS,
           COUNT(DISTINCT CASE WHEN rc.CONTRACT_TYPE = 'Month-to-Month' AND ar.ORGANIZATION_UID IS NOT NULL THEN rc.ORGANIZATION_UID END) AS M2M_CALLED
    FROM renewal_cohort_all rc
    LEFT JOIN renewal_ar_events ar ON rc.ORGANIZATION_UID = ar.ORGANIZATION_UID
    GROUP BY rc.CSM_NAME, rc.RENEWAL_MONTH
),

-- ── Pivot to M1/M2/M3 ──
renewal_m1 AS (SELECT * FROM renewal_coverage_all WHERE RENEWAL_MONTH = DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE))),
renewal_m2 AS (SELECT * FROM renewal_coverage_all WHERE RENEWAL_MONTH = DATE_TRUNC('month', DATEADD('month', 2, '{anchor}'::DATE))),
renewal_m3 AS (SELECT * FROM renewal_coverage_all WHERE RENEWAL_MONTH = DATE_TRUNC('month', DATEADD('month', 3, '{anchor}'::DATE))),

-- ── Segment for composite AR normalization ──
csm_segment AS (
    SELECT CSM_NAME, SEGMENT FROM (
        SELECT m.MONTH_START_ACCOUNT_OWNER AS CSM_NAME, m.ORGANIZATION_ACCOUNT_SEGMENT AS SEGMENT, COUNT(*) AS CNT
        FROM ANALYSIS.FINANCE.SALESFORCE_ORGANIZATION_MONTH_MRR_PLUS_6_MONTHS m
        INNER JOIN csm_allowlist al ON m.MONTH_START_ACCOUNT_OWNER = al.CSM_NAME
        WHERE m.MONTH = DATE_TRUNC('month', '{anchor}'::DATE) AND m.ORGANIZATION_MONTH_START_MRR_USD > 0
        GROUP BY 1, 2
    ) QUALIFY ROW_NUMBER() OVER (PARTITION BY CSM_NAME ORDER BY CNT DESC) = 1
)

SELECT
    s.CSM_NAME,
    -- M1
    rm1.COVERAGE_PCT AS M1_PCT,
    COALESCE(rm1.COHORT_CALLED, 0) AS M1_CALLED, COALESCE(rm1.COHORT_TOTAL, 0) AS M1_TOTAL,
    COALESCE(rm1.M2M_ORGS, 0) AS M1_M2M, COALESCE(rm1.M2M_CALLED, 0) AS M1_M2M_CALLED,
    -- M2
    rm2.COVERAGE_PCT AS M2_PCT,
    COALESCE(rm2.COHORT_CALLED, 0) AS M2_CALLED, COALESCE(rm2.COHORT_TOTAL, 0) AS M2_TOTAL,
    COALESCE(rm2.M2M_ORGS, 0) AS M2_M2M, COALESCE(rm2.M2M_CALLED, 0) AS M2_M2M_CALLED,
    -- M3
    rm3.COVERAGE_PCT AS M3_PCT,
    COALESCE(rm3.COHORT_CALLED, 0) AS M3_CALLED, COALESCE(rm3.COHORT_TOTAL, 0) AS M3_TOTAL,
    COALESCE(rm3.M2M_ORGS, 0) AS M3_M2M, COALESCE(rm3.M2M_CALLED, 0) AS M3_M2M_CALLED,
    -- Renewal coverage = M2
    rm2.COVERAGE_PCT AS RENEWAL_COV,
    -- Recomputed composite (AR + call + SLA from snapshot, UFR from new query)
    ROUND((
        LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE
            WHEN seg.SEGMENT ILIKE '%%phone%%' OR seg.SEGMENT ILIKE '%%platform%%' THEN 2.5
            WHEN seg.SEGMENT ILIKE '%%Mid%%' THEN 3.0
            WHEN seg.SEGMENT ILIKE '%%Strategic%%' THEN 1.5
            ELSE 4.0 END)
        + COALESCE(s.AVG_CALL_SCORE / 100.0, 0)
        + COALESCE(s.RET_SLA_PCT, 0)
        + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
               AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN COALESCE(rm2.COVERAGE_PCT, 0) ELSE 0 END
    ) / (1.0
        + CASE WHEN s.AVG_CALL_SCORE IS NOT NULL THEN 1.0 ELSE 0 END
        + CASE WHEN s.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END
        + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
               AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN 1.0 ELSE 0 END
    ), 4) AS NEW_COMPOSITE,
    -- Status
    CASE
        WHEN ROUND((
            LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE
                WHEN seg.SEGMENT ILIKE '%%phone%%' OR seg.SEGMENT ILIKE '%%platform%%' THEN 2.5
                WHEN seg.SEGMENT ILIKE '%%Mid%%' THEN 3.0
                WHEN seg.SEGMENT ILIKE '%%Strategic%%' THEN 1.5
                ELSE 4.0 END)
            + COALESCE(s.AVG_CALL_SCORE / 100.0, 0)
            + COALESCE(s.RET_SLA_PCT, 0)
            + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
                   AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN COALESCE(rm2.COVERAGE_PCT, 0) ELSE 0 END
        ) / (1.0
            + CASE WHEN s.AVG_CALL_SCORE IS NOT NULL THEN 1.0 ELSE 0 END
            + CASE WHEN s.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END
            + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
                   AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN 1.0 ELSE 0 END
        ), 4) >= 0.80 THEN 'green'
        WHEN ROUND((
            LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE
                WHEN seg.SEGMENT ILIKE '%%phone%%' OR seg.SEGMENT ILIKE '%%platform%%' THEN 2.5
                WHEN seg.SEGMENT ILIKE '%%Mid%%' THEN 3.0
                WHEN seg.SEGMENT ILIKE '%%Strategic%%' THEN 1.5
                ELSE 4.0 END)
            + COALESCE(s.AVG_CALL_SCORE / 100.0, 0)
            + COALESCE(s.RET_SLA_PCT, 0)
            + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
                   AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN COALESCE(rm2.COVERAGE_PCT, 0) ELSE 0 END
        ) / (1.0
            + CASE WHEN s.AVG_CALL_SCORE IS NOT NULL THEN 1.0 ELSE 0 END
            + CASE WHEN s.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END
            + CASE WHEN seg.SEGMENT NOT ILIKE '%%phone%%' AND seg.SEGMENT NOT ILIKE '%%platform%%'
                   AND COALESCE(rm2.COHORT_TOTAL, 0) > 0 THEN 1.0 ELSE 0 END
        ), 4) >= 0.50 THEN 'yellow'
        ELSE 'red'
    END AS NEW_STATUS
FROM ANALYST_SANDBOX.JOSH_ROOKSTOOL.CS_PERF_HUB_SNAPSHOTS s
LEFT JOIN renewal_m1 rm1 ON s.CSM_NAME = rm1.CSM_NAME
LEFT JOIN renewal_m2 rm2 ON s.CSM_NAME = rm2.CSM_NAME
LEFT JOIN renewal_m3 rm3 ON s.CSM_NAME = rm3.CSM_NAME
LEFT JOIN csm_segment seg ON s.CSM_NAME = seg.CSM_NAME
WHERE s.SNAPSHOT_DATE = '{anchor}'
"""


def main():
    print("=" * 65)
    print("  Recompute UFR snapshots: renewal cohort + composite + status")
    print("=" * 65)

    conn = snowflake.connector.connect(
        authenticator="externalbrowser",
        account="rla99487",
        user="josh.rookstool@podium.com",
        database="ANALYST_SANDBOX",
        schema="JOSH_ROOKSTOOL",
    )
    cur = conn.cursor()
    print("Connected\n")

    current = START_DATE
    total = 0
    while current <= END_DATE:
        d = current.isoformat()
        print(f"  {d}: ", end="", flush=True)

        query = UFR_RECOMPUTE_SQL.replace("{anchor}", d)
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

        if rows:
            values = []
            for row in rows:
                r = dict(zip(cols, row))
                csm = r["CSM_NAME"].replace("'", "''")
                m1_pct = f"{r['M1_PCT']}" if r["M1_PCT"] is not None else "NULL"
                m1_called = r["M1_CALLED"] or 0
                m1_total = r["M1_TOTAL"] or 0
                m1_m2m = r["M1_M2M"] or 0
                m1_m2m_c = r["M1_M2M_CALLED"] or 0
                m2_pct = f"{r['M2_PCT']}" if r["M2_PCT"] is not None else "NULL"
                m2_called = r["M2_CALLED"] or 0
                m2_total = r["M2_TOTAL"] or 0
                m2_m2m = r["M2_M2M"] or 0
                m2_m2m_c = r["M2_M2M_CALLED"] or 0
                m3_pct = f"{r['M3_PCT']}" if r["M3_PCT"] is not None else "NULL"
                m3_called = r["M3_CALLED"] or 0
                m3_total = r["M3_TOTAL"] or 0
                m3_m2m = r["M3_M2M"] or 0
                m3_m2m_c = r["M3_M2M_CALLED"] or 0
                ren_cov = f"{r['RENEWAL_COV']}" if r["RENEWAL_COV"] is not None else "NULL"
                comp = f"{r['NEW_COMPOSITE']}" if r["NEW_COMPOSITE"] is not None else "NULL"
                status = r["NEW_STATUS"] or "red"
                values.append(
                    f"('{csm}',"
                    f"{m1_pct},{m1_called},{m1_total},{m1_m2m},{m1_m2m_c},"
                    f"{m2_pct},{m2_called},{m2_total},{m2_m2m},{m2_m2m_c},"
                    f"{m3_pct},{m3_called},{m3_total},{m3_m2m},{m3_m2m_c},"
                    f"{ren_cov},{comp},'{status}')"
                )

            val_str = ",".join(values)
            merge_sql = f"""
                MERGE INTO CS_PERF_HUB_SNAPSHOTS t
                USING (SELECT $1 AS CSM_NAME,
                              $2 AS M1P, $3 AS M1C, $4 AS M1T, $5 AS M1M, $6 AS M1MC,
                              $7 AS M2P, $8 AS M2C, $9 AS M2T, $10 AS M2M, $11 AS M2MC,
                              $12 AS M3P, $13 AS M3C, $14 AS M3T, $15 AS M3M, $16 AS M3MC,
                              $17 AS RCOV, $18 AS COMP, $19 AS STAT
                       FROM VALUES {val_str}) s
                ON t.SNAPSHOT_DATE = '{d}' AND t.CSM_NAME = s.CSM_NAME
                WHEN MATCHED THEN UPDATE SET
                    RENEWAL_M1_PCT = s.M1P, RENEWAL_M1_CALLED = s.M1C, RENEWAL_M1_TOTAL = s.M1T,
                    RENEWAL_M1_M2M = s.M1M, RENEWAL_M1_M2M_CALLED = s.M1MC,
                    RENEWAL_M2_PCT = s.M2P, RENEWAL_M2_CALLED = s.M2C, RENEWAL_M2_TOTAL = s.M2T,
                    RENEWAL_M2_M2M = s.M2M, RENEWAL_M2_M2M_CALLED = s.M2MC,
                    RENEWAL_M3_PCT = s.M3P, RENEWAL_M3_CALLED = s.M3C, RENEWAL_M3_TOTAL = s.M3T,
                    RENEWAL_M3_M2M = s.M3M, RENEWAL_M3_M2M_CALLED = s.M3MC,
                    RENEWAL_COVERAGE_PCT = s.RCOV,
                    COMPOSITE_SCORE = s.COMP,
                    OVERALL_STATUS = s.STAT
            """
            cur.execute(merge_sql)
            total += len(rows)
            print(f"✓ {len(rows)} CSMs updated")
        else:
            print("no rows")

        current += timedelta(days=1)

    print(f"\nDone. {total} total rows recomputed across {(END_DATE - START_DATE).days + 1} days.")
    conn.close()


if __name__ == "__main__":
    main()
