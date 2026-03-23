#!/usr/bin/env python3
"""
Enrich existing CS_PERF_HUB_SNAPSHOTS with raw counts for UFR and Retention SLA.
Runs after backfill to add: RET_SLA_DONE, RET_SLA_TOTAL, RENEWAL_M{1,2,3}_CALLED, RENEWAL_M{1,2,3}_TOTAL.
This avoids recomputing the entire backfill — only computes the missing count columns.
"""

import snowflake.connector
from datetime import date, timedelta

START_DATE = date(2026, 1, 28)
END_DATE = date(2026, 3, 20)

# Retention SLA counts per CSM for a given anchor date
RET_SLA_COUNTS_SQL = """
WITH csm_allowlist AS (
    SELECT DISTINCT SALESFORCE_USER_FULL_NAME AS CSM_NAME
    FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_IS_ACTIVE = TRUE
      AND (SALESFORCE_USER_TITLE ILIKE '%customer success manager%' OR SALESFORCE_USER_TITLE ILIKE 'csm%' OR SALESFORCE_USER_TITLE = 'CSM')
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'Manager%'
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'VP%'
      AND SALESFORCE_USER_FULL_NAME NOT IN ('Maria Lam')
      AND (USER_WORKING_LOCATION IS NULL OR USER_WORKING_LOCATION NOT ILIKE '%australia%')
),
retention_outreach AS (
    SELECT DISTINCT DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) AS EVENT_DATE,
           csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa
    INNER JOIN csm_allowlist al ON csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME = al.CSM_NAME
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed'
      AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) >= DATEADD('day', -30, '{anchor}') AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) < '{anchor}'
    UNION
    SELECT DISTINCT DATE(gc.CALL_EFFECTIVE_START_AT), gc.CALL_SALESFORCE_USER_FULL_NAME, gc.ORGANIZATION_UID
    FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN csm_allowlist al ON gc.CALL_SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.ORGANIZATION_UID IS NOT NULL
      AND gc.CALL_EFFECTIVE_START_AT >= DATEADD('day', -30, '{anchor}') AND gc.CALL_EFFECTIVE_START_AT < '{anchor}'
    UNION
    SELECT DISTINCT DATE(pc.CALL_INSERTED_AT), pc.CALL_MADE_BY_USER_FULL_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID
    FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN csm_allowlist al ON pc.CALL_MADE_BY_USER_FULL_NAME = al.CSM_NAME
    WHERE pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND pc.CALL_INSERTED_AT >= DATEADD('day', -30, '{anchor}') AND pc.CALL_INSERTED_AT < '{anchor}'
)
SELECT c.CASE_OWNER_FULL_NAME AS CSM_NAME,
       COUNT(DISTINCT CASE WHEN ro.ORGANIZATION_UID IS NOT NULL THEN c.CASE_ID END) AS SLA_DONE,
       COUNT(DISTINCT c.CASE_ID) AS SLA_TOTAL
FROM BUILD.SALESFORCE.CORE_CASES c
INNER JOIN csm_allowlist al ON c.CASE_OWNER_FULL_NAME = al.CSM_NAME
LEFT JOIN retention_outreach ro ON c.ORGANIZATION_UID = ro.ORGANIZATION_UID AND ro.EVENT_DATE >= c.CASE_CREATED_AT::DATE
WHERE c.CASE_RECORD_TYPE_NAME = 'Retention'
  AND c.CASE_CREATED_AT >= DATEADD('day', -30, '{anchor}') AND c.CASE_CREATED_AT < '{anchor}'
  AND c.ORGANIZATION_UID IS NOT NULL
  AND (c.CASE_CREATED_BY_ID IS NULL OR c.CASE_CREATED_BY_ID != c.CASE_OWNER_ID)
GROUP BY c.CASE_OWNER_FULL_NAME
"""

# UFR counts per CSM per renewal month for a given anchor date
UFR_COUNTS_SQL = """
WITH csm_allowlist AS (
    SELECT DISTINCT SALESFORCE_USER_FULL_NAME AS CSM_NAME
    FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_IS_ACTIVE = TRUE
      AND (SALESFORCE_USER_TITLE ILIKE '%customer success manager%' OR SALESFORCE_USER_TITLE ILIKE 'csm%' OR SALESFORCE_USER_TITLE = 'CSM')
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'Manager%'
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'VP%'
      AND SALESFORCE_USER_FULL_NAME NOT IN ('Maria Lam')
      AND (USER_WORKING_LOCATION IS NULL OR USER_WORKING_LOCATION NOT ILIKE '%australia%')
),
renewal_cohort_all AS (
    SELECT DISTINCT m.MONTH_START_ACCOUNT_OWNER AS CSM_NAME, m.ORGANIZATION_UID,
           DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) AS RENEWAL_MONTH
    FROM ANALYSIS.FINANCE.SALESFORCE_ORGANIZATION_MONTH_MRR_PLUS_6_MONTHS m
    INNER JOIN csm_allowlist al ON m.MONTH_START_ACCOUNT_OWNER = al.CSM_NAME
    WHERE m.MONTH = DATE_TRUNC('month', '{anchor}'::DATE)
      AND m.ORGANIZATION_MONTH_START_MRR_USD > 0
      AND DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) IN (
          DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)),
          DATE_TRUNC('month', DATEADD('month', 2, '{anchor}'::DATE)),
          DATE_TRUNC('month', DATEADD('month', 3, '{anchor}'::DATE))
      )
),
renewal_contacted AS (
    SELECT DISTINCT csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa
    INNER JOIN renewal_cohort_all rc ON csa.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed'
      AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) < '{anchor}'
    UNION
    SELECT DISTINCT gc.CALL_SALESFORCE_USER_FULL_NAME, gc.ORGANIZATION_UID
    FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN renewal_cohort_all rc ON gc.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.CALL_ELAPSED_MINUTES >= 15 AND gc.ORGANIZATION_UID IS NOT NULL
      AND DATE(gc.CALL_EFFECTIVE_START_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND DATE(gc.CALL_EFFECTIVE_START_AT) < '{anchor}'
    UNION
    SELECT DISTINCT pc.CALL_MADE_BY_USER_FULL_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID
    FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN renewal_cohort_all rc ON pc.CALL_MADE_TO_ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE pc.CALL_DURATION_SECONDS >= 900 AND pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND DATE(pc.CALL_INSERTED_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH)
      AND DATE(pc.CALL_INSERTED_AT) < '{anchor}'
)
SELECT rc.CSM_NAME, rc.RENEWAL_MONTH,
       COUNT(DISTINCT CASE WHEN rac.ORGANIZATION_UID IS NOT NULL THEN rc.ORGANIZATION_UID END) AS CALLED,
       COUNT(DISTINCT rc.ORGANIZATION_UID) AS TOTAL
FROM renewal_cohort_all rc
LEFT JOIN renewal_contacted rac ON rc.ORGANIZATION_UID = rac.ORGANIZATION_UID AND rc.CSM_NAME = rac.CSM_NAME
GROUP BY rc.CSM_NAME, rc.RENEWAL_MONTH
"""


def main():
    print("=" * 65)
    print("  CS Performance Hub — Snapshot Enrichment (UFR + SLA counts)")
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
    total_updates = 0
    while current <= END_DATE:
        d = current.isoformat()
        print(f"  {d}: ", end="", flush=True)

        # Retention SLA counts
        sla_query = RET_SLA_COUNTS_SQL.replace("{anchor}", d)
        cur.execute(sla_query)
        sla_rows = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        # UFR counts
        ufr_query = UFR_COUNTS_SQL.replace("{anchor}", d)
        cur.execute(ufr_query)
        ufr_rows = {}
        for row in cur.fetchall():
            csm, month, called, total = row
            if csm not in ufr_rows:
                ufr_rows[csm] = {}
            ufr_rows[csm][str(month)[:7]] = (called, total)

        # Compute month keys
        anchor_date = current
        m1_key = (anchor_date.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m')
        m2_date = (anchor_date.replace(day=1) + timedelta(days=63)).replace(day=1)
        m2_key = m2_date.strftime('%Y-%m')
        m3_date = (anchor_date.replace(day=1) + timedelta(days=93)).replace(day=1)
        m3_key = m3_date.strftime('%Y-%m')

        # Build a temp table with all counts for this date and MERGE in one shot
        values = []
        cur.execute(f"SELECT CSM_NAME FROM CS_PERF_HUB_SNAPSHOTS WHERE SNAPSHOT_DATE = '{d}'")
        csms = [row[0] for row in cur.fetchall()]

        for csm in csms:
            sla_done, sla_total = sla_rows.get(csm, (0, 0))
            ufr = ufr_rows.get(csm, {})
            m1_called, m1_total = ufr.get(m1_key, (0, 0))
            m2_called, m2_total = ufr.get(m2_key, (0, 0))
            m3_called, m3_total = ufr.get(m3_key, (0, 0))
            csm_escaped = csm.replace("'", "''")
            values.append(f"('{csm_escaped}',{sla_done},{sla_total},{m1_called},{m1_total},{m2_called},{m2_total},{m3_called},{m3_total})")

        if values:
            # Batch UPDATE via MERGE
            val_str = ','.join(values)
            cur.execute(f"""
                MERGE INTO CS_PERF_HUB_SNAPSHOTS t
                USING (SELECT $1 AS CSM_NAME, $2 AS SD, $3 AS ST, $4 AS M1C, $5 AS M1T, $6 AS M2C, $7 AS M2T, $8 AS M3C, $9 AS M3T
                       FROM VALUES {val_str}) s
                ON t.SNAPSHOT_DATE = '{d}' AND t.CSM_NAME = s.CSM_NAME
                WHEN MATCHED THEN UPDATE SET
                    RET_SLA_DONE = s.SD, RET_SLA_TOTAL = s.ST,
                    RENEWAL_M1_CALLED = s.M1C, RENEWAL_M1_TOTAL = s.M1T,
                    RENEWAL_M2_CALLED = s.M2C, RENEWAL_M2_TOTAL = s.M2T,
                    RENEWAL_M3_CALLED = s.M3C, RENEWAL_M3_TOTAL = s.M3T
            """)

        total_updates += len(csms)
        print(f"✓ {len(csms)} CSMs enriched (SLA: {len(sla_rows)} with cases, UFR: {len(ufr_rows)} with cohorts)")

        current += timedelta(days=1)

    print(f"\nDone. {total_updates} rows enriched.")
    conn.close()


if __name__ == "__main__":
    main()
