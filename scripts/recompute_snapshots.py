#!/usr/bin/env python3
"""
Recompute historical snapshot columns that changed methodology:
- AVG_CALL_SCORE: L90 → L30
- RET_SLA_PCT/DONE/TOTAL: add 2 business day constraint + grace
- COMPOSITE_SCORE: recompute from updated pillars
- OVERALL_STATUS: recompute from updated composite
"""

import snowflake.connector
from datetime import date, timedelta

START_DATE = date(2026, 1, 28)
END_DATE = date(2026, 3, 23)

# Recompute call score (L30) and ret SLA (2 biz day) per CSM for a given anchor
RECOMPUTE_SQL = """
WITH csm_allowlist AS (
    SELECT DISTINCT SALESFORCE_USER_FULL_NAME AS CSM_NAME
    FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_IS_ACTIVE = TRUE
      AND (SALESFORCE_USER_TITLE ILIKE '%customer success manager%' OR SALESFORCE_USER_TITLE ILIKE 'csm%' OR SALESFORCE_USER_TITLE = 'CSM')
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'Manager%' AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'VP%'
      AND SALESFORCE_USER_FULL_NAME NOT IN ('Maria Lam')
      AND (USER_WORKING_LOCATION IS NULL OR USER_WORKING_LOCATION NOT ILIKE '%australia%')
    UNION ALL
    SELECT SALESFORCE_USER_FULL_NAME FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_FULL_NAME IN ('Chris Isham') AND SALESFORCE_USER_IS_ACTIVE = TRUE
),

-- Call score L30
call_scores_l30 AS (
    SELECT s.CALL_SALESFORCE_USER_FULL_NAME AS CSM_NAME,
           ROUND(AVG(s.FINAL_ADJUSTED_SCORE), 1) AS AVG_CALL_SCORE,
           ROUND(AVG(s.FINAL_ADJUSTED_SCORE) / 100.0, 4) AS CALL_SCORE_NORM
    FROM (
        SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE
        FROM ANALYSIS.CUSTOMER_SUCCESS.HVAC_CALL_SCORING_PROCESSED
        WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE >= DATEADD('day', -30, '{anchor}') AND CONVERSATION_DATE < '{anchor}'
        UNION ALL
        SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE
        FROM ANALYSIS.CUSTOMER_SUCCESS.AUTOMOTIVE_CALL_SCORING_PROCESSED
        WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE >= DATEADD('day', -30, '{anchor}') AND CONVERSATION_DATE < '{anchor}'
        UNION ALL
        SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE
        FROM ANALYSIS.CUSTOMER_SUCCESS.MEDSPA_CALL_SCORING_PROCESSED
        WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE >= DATEADD('day', -30, '{anchor}') AND CONVERSATION_DATE < '{anchor}'
        UNION ALL
        SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE
        FROM ANALYSIS.CUSTOMER_SUCCESS.ACCOUNT_REVIEW_GRADING_EMERGING_MARKETS_PROCESSED
        WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE >= DATEADD('day', -30, '{anchor}') AND CONVERSATION_DATE < '{anchor}'
    ) s
    INNER JOIN csm_allowlist al ON s.CALL_SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    GROUP BY s.CALL_SALESFORCE_USER_FULL_NAME
),

-- Outreach events (any duration, L30, < anchor) for ret SLA
outreach_l30 AS (
    SELECT DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) AS EVENT_DATE,
           csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa
    INNER JOIN csm_allowlist al ON csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME = al.CSM_NAME
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed'
      AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) >= DATEADD('day', -30, '{anchor}')
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) < '{anchor}'
    UNION ALL
    SELECT DATE(gc.CALL_EFFECTIVE_START_AT), gc.CALL_SALESFORCE_USER_FULL_NAME, gc.ORGANIZATION_UID
    FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN csm_allowlist al ON gc.CALL_SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.ORGANIZATION_UID IS NOT NULL
      AND gc.CALL_EFFECTIVE_START_AT >= DATEADD('day', -30, '{anchor}') AND gc.CALL_EFFECTIVE_START_AT < '{anchor}'
    UNION ALL
    SELECT DATE(pc.CALL_INSERTED_AT), pc.CALL_MADE_BY_USER_FULL_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID
    FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN csm_allowlist al ON pc.CALL_MADE_BY_USER_FULL_NAME = al.CSM_NAME
    WHERE pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND pc.CALL_INSERTED_AT >= DATEADD('day', -30, '{anchor}') AND pc.CALL_INSERTED_AT < '{anchor}'
),

-- Retention SLA with 2 business day constraint + grace
retention_tasks AS (
    SELECT c.CASE_ID AS TASK_UID, c.ORGANIZATION_UID, c.CASE_OWNER_FULL_NAME AS CSM_NAME,
           c.CASE_CREATED_AT::DATE AS TASK_DATE,
           DATEADD('day', CASE DAYOFWEEK(c.CASE_CREATED_AT::DATE)
               WHEN 0 THEN 2 WHEN 4 THEN 4 WHEN 5 THEN 4 WHEN 6 THEN 3 ELSE 2
           END, c.CASE_CREATED_AT::DATE) AS SLA_DEADLINE
    FROM BUILD.SALESFORCE.CORE_CASES c
    INNER JOIN csm_allowlist al ON c.CASE_OWNER_FULL_NAME = al.CSM_NAME
    WHERE c.CASE_RECORD_TYPE_NAME = 'Retention'
      AND c.CASE_CREATED_AT >= DATEADD('day', -30, '{anchor}') AND c.CASE_CREATED_AT < '{anchor}'
      AND c.ORGANIZATION_UID IS NOT NULL
      AND (c.CASE_CREATED_BY_ID IS NULL OR c.CASE_CREATED_BY_ID != c.CASE_OWNER_ID)
),
retention_eligible AS (
    SELECT * FROM retention_tasks WHERE SLA_DEADLINE < '{anchor}'::DATE
),
ret_sla AS (
    SELECT rt.CSM_NAME,
           COUNT(DISTINCT rt.TASK_UID) AS SLA_TOTAL,
           COUNT(DISTINCT CASE WHEN oe.ORGANIZATION_UID IS NOT NULL THEN rt.TASK_UID END) AS SLA_DONE,
           ROUND(COUNT(DISTINCT CASE WHEN oe.ORGANIZATION_UID IS NOT NULL THEN rt.TASK_UID END)::FLOAT
                 / NULLIF(COUNT(DISTINCT rt.TASK_UID), 0), 3) AS RET_SLA_PCT
    FROM retention_eligible rt
    LEFT JOIN outreach_l30 oe
        ON rt.ORGANIZATION_UID = oe.ORGANIZATION_UID
        AND oe.EVENT_DATE >= rt.TASK_DATE AND oe.EVENT_DATE <= rt.SLA_DEADLINE
    GROUP BY rt.CSM_NAME
),

-- Segment for composite AR normalization
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
    cs_l30.AVG_CALL_SCORE,
    cs_l30.CALL_SCORE_NORM,
    rs.RET_SLA_PCT,
    COALESCE(rs.SLA_DONE, 0) AS SLA_DONE,
    COALESCE(rs.SLA_TOTAL, 0) AS SLA_TOTAL,
    -- Recompute composite: AR (from existing snapshot) + new call + new SLA + UFR (from existing)
    ROUND((
        LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE
            WHEN seg.SEGMENT ILIKE '%phone%' OR seg.SEGMENT ILIKE '%platform%' THEN 2.5
            WHEN seg.SEGMENT ILIKE '%Mid%' THEN 3.0 WHEN seg.SEGMENT ILIKE '%Strategic%' THEN 1.5 ELSE 4.0 END)
        + COALESCE(cs_l30.CALL_SCORE_NORM, 0)
        + COALESCE(rs.RET_SLA_PCT, 0)
        + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%'
               AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN COALESCE(s.RENEWAL_M2_PCT, 0) ELSE 0 END
    ) / (1.0
        + CASE WHEN cs_l30.CALL_SCORE_NORM IS NOT NULL THEN 1.0 ELSE 0 END
        + CASE WHEN rs.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END
        + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%'
               AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN 1.0 ELSE 0 END
    ), 4) AS NEW_COMPOSITE,
    CASE
        WHEN ROUND((LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE WHEN seg.SEGMENT ILIKE '%phone%' OR seg.SEGMENT ILIKE '%platform%' THEN 2.5 WHEN seg.SEGMENT ILIKE '%Mid%' THEN 3.0 WHEN seg.SEGMENT ILIKE '%Strategic%' THEN 1.5 ELSE 4.0 END) + COALESCE(cs_l30.CALL_SCORE_NORM, 0) + COALESCE(rs.RET_SLA_PCT, 0) + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%' AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN COALESCE(s.RENEWAL_M2_PCT, 0) ELSE 0 END) / (1.0 + CASE WHEN cs_l30.CALL_SCORE_NORM IS NOT NULL THEN 1.0 ELSE 0 END + CASE WHEN rs.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%' AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN 1.0 ELSE 0 END), 4) >= 0.80 THEN 'green'
        WHEN ROUND((LEAST(1.0, COALESCE(s.ARS_PER_DAY, 0) / CASE WHEN seg.SEGMENT ILIKE '%phone%' OR seg.SEGMENT ILIKE '%platform%' THEN 2.5 WHEN seg.SEGMENT ILIKE '%Mid%' THEN 3.0 WHEN seg.SEGMENT ILIKE '%Strategic%' THEN 1.5 ELSE 4.0 END) + COALESCE(cs_l30.CALL_SCORE_NORM, 0) + COALESCE(rs.RET_SLA_PCT, 0) + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%' AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN COALESCE(s.RENEWAL_M2_PCT, 0) ELSE 0 END) / (1.0 + CASE WHEN cs_l30.CALL_SCORE_NORM IS NOT NULL THEN 1.0 ELSE 0 END + CASE WHEN rs.RET_SLA_PCT IS NOT NULL THEN 1.0 ELSE 0 END + CASE WHEN seg.SEGMENT NOT ILIKE '%phone%' AND seg.SEGMENT NOT ILIKE '%platform%' AND COALESCE(s.RENEWAL_M2_PCT, 0) > 0 THEN 1.0 ELSE 0 END), 4) >= 0.50 THEN 'yellow'
        ELSE 'red'
    END AS NEW_STATUS
FROM ANALYST_SANDBOX.JOSH_ROOKSTOOL.CS_PERF_HUB_SNAPSHOTS s
LEFT JOIN call_scores_l30 cs_l30 ON s.CSM_NAME = cs_l30.CSM_NAME
LEFT JOIN ret_sla rs ON s.CSM_NAME = rs.CSM_NAME
LEFT JOIN csm_segment seg ON s.CSM_NAME = seg.CSM_NAME
WHERE s.SNAPSHOT_DATE = '{anchor}'
"""


def main():
    print("=" * 65)
    print("  Recompute snapshots: L30 call score + 2 biz day SLA + composite")
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

        query = RECOMPUTE_SQL.replace("{anchor}", d)
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

        if rows:
            # Build batch MERGE
            values = []
            for row in rows:
                r = dict(zip(cols, row))
                csm = r["CSM_NAME"].replace("'", "''")
                call = f"{r['AVG_CALL_SCORE']}" if r["AVG_CALL_SCORE"] is not None else "NULL"
                sla_pct = f"{r['RET_SLA_PCT']}" if r["RET_SLA_PCT"] is not None else "NULL"
                sla_done = r["SLA_DONE"] or 0
                sla_total = r["SLA_TOTAL"] or 0
                comp = f"{r['NEW_COMPOSITE']}" if r["NEW_COMPOSITE"] is not None else "NULL"
                status = r["NEW_STATUS"] or "red"
                values.append(f"('{csm}',{call},{sla_pct},{sla_done},{sla_total},{comp},'{status}')")

            val_str = ",".join(values)
            cur.execute(f"""
                MERGE INTO CS_PERF_HUB_SNAPSHOTS t
                USING (SELECT $1 AS CSM_NAME, $2 AS CALL, $3 AS SLA, $4 AS SD, $5 AS ST, $6 AS COMP, $7 AS STAT
                       FROM VALUES {val_str}) s
                ON t.SNAPSHOT_DATE = '{d}' AND t.CSM_NAME = s.CSM_NAME
                WHEN MATCHED THEN UPDATE SET
                    AVG_CALL_SCORE = s.CALL,
                    RET_SLA_PCT = s.SLA,
                    RET_SLA_DONE = s.SD,
                    RET_SLA_TOTAL = s.ST,
                    COMPOSITE_SCORE = s.COMP,
                    OVERALL_STATUS = s.STAT
            """)
            total += len(rows)
            print(f"✓ {len(rows)} CSMs updated")
        else:
            print("no rows")

        current += timedelta(days=1)

    print(f"\nDone. {total} rows recomputed.")
    conn.close()


if __name__ == "__main__":
    main()
