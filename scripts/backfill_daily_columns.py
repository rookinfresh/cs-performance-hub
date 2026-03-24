#!/usr/bin/env python3
"""
Backfill daily metric columns + M2M data for historical snapshots.
Targeted UPDATE — only touches rows where DAILY_AR_COUNT IS NULL (not yet backfilled).
Does NOT duplicate or re-insert any rows.

Columns updated:
  DAILY_AR_COUNT, DAILY_ACTIVE, DAILY_CALL_SCORE_SUM, DAILY_CALL_COUNT,
  DAILY_RET_CREATED, DAILY_RET_MET,
  RENEWAL_M1_M2M, RENEWAL_M1_M2M_CALLED, RENEWAL_M2_M2M, RENEWAL_M2_M2M_CALLED,
  RENEWAL_M3_M2M, RENEWAL_M3_M2M_CALLED
"""

import snowflake.connector
from datetime import date, timedelta

DAILY_SQL = """
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
-- SFDC ARs for this day
day_sfdc AS (
    SELECT csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa
    INNER JOIN csm_allowlist al ON csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME = al.CSM_NAME
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed'
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) = '{day}'
),
-- Gong >=15min deduped
day_gong AS (
    SELECT gc.CALL_SALESFORCE_USER_FULL_NAME AS CSM_NAME, gc.ORGANIZATION_UID
    FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN csm_allowlist al ON gc.CALL_SALESFORCE_USER_FULL_NAME = al.CSM_NAME
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.CALL_ELAPSED_MINUTES >= 15 AND gc.ORGANIZATION_UID IS NOT NULL
      AND DATE(gc.CALL_EFFECTIVE_START_AT) = '{day}'
      AND NOT EXISTS (SELECT 1 FROM day_sfdc s WHERE s.CSM_NAME = gc.CALL_SALESFORCE_USER_FULL_NAME AND s.ORGANIZATION_UID = gc.ORGANIZATION_UID)
),
-- Podium >=15min deduped
day_podium AS (
    SELECT pc.CALL_MADE_BY_USER_FULL_NAME AS CSM_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID AS ORGANIZATION_UID
    FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN csm_allowlist al ON pc.CALL_MADE_BY_USER_FULL_NAME = al.CSM_NAME
    WHERE pc.CALL_DURATION_SECONDS >= 900 AND pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND DATE(pc.CALL_INSERTED_AT) = '{day}'
      AND NOT EXISTS (SELECT 1 FROM day_sfdc s WHERE s.CSM_NAME = pc.CALL_MADE_BY_USER_FULL_NAME AND s.ORGANIZATION_UID = pc.CALL_MADE_TO_ORGANIZATION_UID)
),
daily_ar AS (
    SELECT CSM_NAME, COUNT(*) AS AR_COUNT FROM (
        SELECT CSM_NAME, ORGANIZATION_UID FROM day_sfdc UNION ALL SELECT CSM_NAME, ORGANIZATION_UID FROM day_gong UNION ALL SELECT CSM_NAME, ORGANIZATION_UID FROM day_podium
    ) GROUP BY CSM_NAME
),
-- Active: any outreach (any duration) on a weekday
daily_active AS (
    SELECT CSM_NAME, 1 AS IS_ACTIVE FROM (
        SELECT CSM_NAME FROM day_sfdc
        UNION SELECT gc.CALL_SALESFORCE_USER_FULL_NAME AS CSM_NAME FROM BUILD.GONG.CORE_CONVERSATIONS gc WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.ORGANIZATION_UID IS NOT NULL AND DATE(gc.CALL_EFFECTIVE_START_AT) = '{day}' AND gc.CALL_SALESFORCE_USER_FULL_NAME IN (SELECT CSM_NAME FROM csm_allowlist)
        UNION SELECT pc.CALL_MADE_BY_USER_FULL_NAME AS CSM_NAME FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc WHERE pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL AND DATE(pc.CALL_INSERTED_AT) = '{day}' AND pc.CALL_MADE_BY_USER_FULL_NAME IN (SELECT CSM_NAME FROM csm_allowlist)
    ) sub WHERE DAYOFWEEK('{day}'::DATE) NOT IN (0, 6)
    GROUP BY CSM_NAME
),
-- Call scores for this day
daily_calls AS (
    SELECT s.CSM_NAME, SUM(s.CALL_SCORE) AS SCORE_SUM, COUNT(*) AS CALL_COUNT FROM (
        SELECT CALL_SALESFORCE_USER_FULL_NAME AS CSM_NAME, FINAL_ADJUSTED_SCORE AS CALL_SCORE FROM ANALYSIS.CUSTOMER_SUCCESS.HVAC_CALL_SCORING_PROCESSED WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE = '{day}'
        UNION ALL SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE FROM ANALYSIS.CUSTOMER_SUCCESS.AUTOMOTIVE_CALL_SCORING_PROCESSED WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE = '{day}'
        UNION ALL SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE FROM ANALYSIS.CUSTOMER_SUCCESS.MEDSPA_CALL_SCORING_PROCESSED WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE = '{day}'
        UNION ALL SELECT CALL_SALESFORCE_USER_FULL_NAME, FINAL_ADJUSTED_SCORE FROM ANALYSIS.CUSTOMER_SUCCESS.ACCOUNT_REVIEW_GRADING_EMERGING_MARKETS_PROCESSED WHERE INTERACTION_QUALITY != 'incomplete' AND CONVERSATION_DATE = '{day}'
    ) s WHERE s.CSM_NAME IN (SELECT CSM_NAME FROM csm_allowlist) GROUP BY s.CSM_NAME
),
-- Retention cases whose SLA window closed on this day
daily_ret AS (
    SELECT rt.CSM_NAME, COUNT(DISTINCT rt.CASE_ID) AS RET_CREATED,
           COUNT(DISTINCT CASE WHEN oe.ORGANIZATION_UID IS NOT NULL THEN rt.CASE_ID END) AS RET_MET
    FROM (
        SELECT c.CASE_ID, c.ORGANIZATION_UID, c.CASE_OWNER_FULL_NAME AS CSM_NAME, c.CASE_CREATED_AT::DATE AS TASK_DATE,
               DATEADD('day', CASE DAYOFWEEK(c.CASE_CREATED_AT::DATE) WHEN 0 THEN 2 WHEN 4 THEN 4 WHEN 5 THEN 4 WHEN 6 THEN 3 ELSE 2 END, c.CASE_CREATED_AT::DATE) AS SLA_DEADLINE
        FROM BUILD.SALESFORCE.CORE_CASES c
        INNER JOIN csm_allowlist al ON c.CASE_OWNER_FULL_NAME = al.CSM_NAME
        WHERE c.CASE_RECORD_TYPE_NAME = 'Retention' AND c.ORGANIZATION_UID IS NOT NULL
          AND (c.CASE_CREATED_BY_ID IS NULL OR c.CASE_CREATED_BY_ID != c.CASE_OWNER_ID)
    ) rt
    LEFT JOIN (
        SELECT DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) AS EVENT_DATE, csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
        FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed' AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL AND csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME IN (SELECT CSM_NAME FROM csm_allowlist)
        UNION ALL SELECT DATE(gc.CALL_EFFECTIVE_START_AT), gc.CALL_SALESFORCE_USER_FULL_NAME, gc.ORGANIZATION_UID FROM BUILD.GONG.CORE_CONVERSATIONS gc WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.ORGANIZATION_UID IS NOT NULL AND gc.CALL_SALESFORCE_USER_FULL_NAME IN (SELECT CSM_NAME FROM csm_allowlist)
        UNION ALL SELECT DATE(pc.CALL_INSERTED_AT), pc.CALL_MADE_BY_USER_FULL_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc WHERE pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL AND pc.CALL_MADE_BY_USER_FULL_NAME IN (SELECT CSM_NAME FROM csm_allowlist)
    ) oe ON rt.ORGANIZATION_UID = oe.ORGANIZATION_UID AND oe.EVENT_DATE >= rt.TASK_DATE AND oe.EVENT_DATE <= rt.SLA_DEADLINE
    WHERE rt.SLA_DEADLINE = '{day}'
    GROUP BY rt.CSM_NAME
)
SELECT s.CSM_NAME,
       COALESCE(da.AR_COUNT, 0) AS DAILY_AR_COUNT,
       COALESCE(dact.IS_ACTIVE, 0) AS DAILY_ACTIVE,
       dc.SCORE_SUM AS DAILY_CALL_SCORE_SUM,
       COALESCE(dc.CALL_COUNT, 0) AS DAILY_CALL_COUNT,
       COALESCE(dr.RET_CREATED, 0) AS DAILY_RET_CREATED,
       COALESCE(dr.RET_MET, 0) AS DAILY_RET_MET
FROM CS_PERF_HUB_SNAPSHOTS s
LEFT JOIN daily_ar da ON s.CSM_NAME = da.CSM_NAME
LEFT JOIN daily_active dact ON s.CSM_NAME = dact.CSM_NAME
LEFT JOIN daily_calls dc ON s.CSM_NAME = dc.CSM_NAME
LEFT JOIN daily_ret dr ON s.CSM_NAME = dr.CSM_NAME
WHERE s.SNAPSHOT_DATE = '{snap_date}'
"""

M2M_SQL = """
WITH csm_allowlist AS (
    SELECT DISTINCT SALESFORCE_USER_FULL_NAME AS CSM_NAME FROM BUILD.SALESFORCE.CORE_USERS
    WHERE SALESFORCE_USER_IS_ACTIVE = TRUE
      AND (SALESFORCE_USER_TITLE ILIKE '%customer success manager%' OR SALESFORCE_USER_TITLE ILIKE 'csm%' OR SALESFORCE_USER_TITLE = 'CSM')
      AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'Manager%' AND SALESFORCE_USER_ROLE_NAME NOT ILIKE 'VP%'
      AND SALESFORCE_USER_FULL_NAME NOT IN ('Maria Lam')
      AND (USER_WORKING_LOCATION IS NULL OR USER_WORKING_LOCATION NOT ILIKE '%australia%')
    UNION ALL SELECT SALESFORCE_USER_FULL_NAME FROM BUILD.SALESFORCE.CORE_USERS WHERE SALESFORCE_USER_FULL_NAME IN ('Chris Isham') AND SALESFORCE_USER_IS_ACTIVE = TRUE
),
renewal_cohort AS (
    SELECT DISTINCT m.MONTH_START_ACCOUNT_OWNER AS CSM_NAME, m.ORGANIZATION_UID,
           DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) AS RENEWAL_MONTH,
           m.MONTH_START_CONTRACT_TYPE_BUCKET AS CONTRACT_TYPE
    FROM ANALYSIS.FINANCE.SALESFORCE_ORGANIZATION_MONTH_MRR_PLUS_6_MONTHS m
    INNER JOIN csm_allowlist al ON m.MONTH_START_ACCOUNT_OWNER = al.CSM_NAME
    WHERE m.MONTH = DATE_TRUNC('month', '{anchor}'::DATE) AND m.ORGANIZATION_MONTH_START_MRR_USD > 0
      AND DATE_TRUNC('month', m.MONTH_START_CONTRACT_END_MONTH) IN (
          DATE_TRUNC('month', DATEADD('month', 1, '{anchor}'::DATE)),
          DATE_TRUNC('month', DATEADD('month', 2, '{anchor}'::DATE)),
          DATE_TRUNC('month', DATEADD('month', 3, '{anchor}'::DATE)))
),
renewal_contacted AS (
    SELECT DISTINCT csa.CUSTOMER_SUCCESS_ACTION_OWNER_FULL_NAME AS CSM_NAME, csa.ORGANIZATION_UID
    FROM BUILD.SALESFORCE.CORE_CUSTOMER_SUCCESS_ACTIONS csa INNER JOIN renewal_cohort rc ON csa.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE csa.RECORD_TYPE_ID = '0125G000001QG3fQAG' AND LOWER(csa.CUSTOMER_SUCCESS_ACTION_STATUS) = 'closed' AND csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT IS NOT NULL
      AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH) AND DATE(csa.CUSTOMER_SUCCESS_ACTION_CLOSED_AT) < '{anchor}'
    UNION
    SELECT DISTINCT gc.CALL_SALESFORCE_USER_FULL_NAME, gc.ORGANIZATION_UID FROM BUILD.GONG.CORE_CONVERSATIONS gc
    INNER JOIN renewal_cohort rc ON gc.ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE LOWER(gc.CALL_STATUS) = 'completed' AND gc.CALL_ELAPSED_MINUTES >= 15 AND gc.ORGANIZATION_UID IS NOT NULL
      AND DATE(gc.CALL_EFFECTIVE_START_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH) AND DATE(gc.CALL_EFFECTIVE_START_AT) < '{anchor}'
    UNION
    SELECT DISTINCT pc.CALL_MADE_BY_USER_FULL_NAME, pc.CALL_MADE_TO_ORGANIZATION_UID FROM BUILD.PODIUM_INTERNAL.CORE_CALLS pc
    INNER JOIN renewal_cohort rc ON pc.CALL_MADE_TO_ORGANIZATION_UID = rc.ORGANIZATION_UID
    WHERE pc.CALL_DURATION_SECONDS >= 900 AND pc.CALL_MADE_TO_ORGANIZATION_UID IS NOT NULL
      AND DATE(pc.CALL_INSERTED_AT) >= DATEADD('day', -120, rc.RENEWAL_MONTH) AND DATE(pc.CALL_INSERTED_AT) < '{anchor}'
)
SELECT rc.CSM_NAME, rc.RENEWAL_MONTH,
       COUNT(DISTINCT CASE WHEN rc.CONTRACT_TYPE = 'Month-to-Month' THEN rc.ORGANIZATION_UID END) AS M2M_TOTAL,
       COUNT(DISTINCT CASE WHEN rc.CONTRACT_TYPE = 'Month-to-Month' AND rac.ORGANIZATION_UID IS NOT NULL THEN rc.ORGANIZATION_UID END) AS M2M_CALLED
FROM renewal_cohort rc
LEFT JOIN renewal_contacted rac ON rc.ORGANIZATION_UID = rac.ORGANIZATION_UID AND rc.CSM_NAME = rac.CSM_NAME
GROUP BY rc.CSM_NAME, rc.RENEWAL_MONTH
"""


def main():
    print("=" * 65)
    print("  Backfill daily columns + M2M data for historical snapshots")
    print("=" * 65)

    conn = snowflake.connector.connect(
        authenticator="externalbrowser", account="rla99487",
        user="josh.rookstool@podium.com", database="ANALYST_SANDBOX", schema="JOSH_ROOKSTOOL",
    )
    cur = conn.cursor()
    print("Connected\n")

    # Find dates that need backfill (DAILY_AR_COUNT IS NULL)
    cur.execute("SELECT DISTINCT SNAPSHOT_DATE FROM CS_PERF_HUB_SNAPSHOTS WHERE DAILY_AR_COUNT IS NULL ORDER BY 1")
    dates_to_fill = [row[0] for row in cur.fetchall()]
    print(f"Found {len(dates_to_fill)} dates needing backfill\n")

    for snap_date in dates_to_fill:
        d = snap_date.isoformat() if hasattr(snap_date, 'isoformat') else str(snap_date)
        # Daily metrics use the day BEFORE the snapshot (snapshot captures yesterday's activity)
        day_before = (snap_date - timedelta(days=1)).isoformat() if hasattr(snap_date, 'isoformat') else d
        print(f"  {d}: ", end="", flush=True)

        # 1. Daily columns (AR, active, calls, ret)
        query = DAILY_SQL.replace("{day}", day_before).replace("{snap_date}", d)
        cur.execute(query)
        daily_rows = {row[0]: row[1:] for row in cur.fetchall()}

        # 2. M2M data
        m2m_query = M2M_SQL.replace("{anchor}", d)
        cur.execute(m2m_query)
        m2m_rows = {}
        for row in cur.fetchall():
            csm, month, m2m_total, m2m_called = row
            if csm not in m2m_rows:
                m2m_rows[csm] = {}
            m2m_rows[csm][str(month)[:7]] = (m2m_total, m2m_called)

        # Compute month keys
        snap_dt = snap_date if hasattr(snap_date, 'month') else date.fromisoformat(d)
        m1_key = (snap_dt.replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m')
        m2_key = (snap_dt.replace(day=1) + timedelta(days=63)).replace(day=1).strftime('%Y-%m')
        m3_key = (snap_dt.replace(day=1) + timedelta(days=93)).replace(day=1).strftime('%Y-%m')

        # Build MERGE values
        cur.execute(f"SELECT CSM_NAME FROM CS_PERF_HUB_SNAPSHOTS WHERE SNAPSHOT_DATE = '{d}'")
        csms = [row[0] for row in cur.fetchall()]

        values = []
        for csm in csms:
            dr = daily_rows.get(csm, (0, 0, None, 0, 0, 0))
            m2m = m2m_rows.get(csm, {})
            m1_m2m, m1_m2m_called = m2m.get(m1_key, (0, 0))
            m2_m2m, m2_m2m_called = m2m.get(m2_key, (0, 0))
            m3_m2m, m3_m2m_called = m2m.get(m3_key, (0, 0))
            ce = csm.replace("'", "''")
            cs_sum = f"{dr[2]}" if dr[2] is not None else "NULL"
            values.append(f"('{ce}',{dr[0]},{dr[1]},{cs_sum},{dr[3]},{dr[4]},{dr[5]},{m1_m2m},{m1_m2m_called},{m2_m2m},{m2_m2m_called},{m3_m2m},{m3_m2m_called})")

        if values:
            val_str = ",".join(values)
            cur.execute(f"""
                MERGE INTO CS_PERF_HUB_SNAPSHOTS t
                USING (SELECT $1 AS CSM_NAME, $2 AS D_AR, $3 AS D_ACT, $4 AS D_CS, $5 AS D_CC, $6 AS D_RC, $7 AS D_RM,
                              $8 AS M1M, $9 AS M1MC, $10 AS M2M, $11 AS M2MC, $12 AS M3M, $13 AS M3MC
                       FROM VALUES {val_str}) s
                ON t.SNAPSHOT_DATE = '{d}' AND t.CSM_NAME = s.CSM_NAME
                WHEN MATCHED THEN UPDATE SET
                    DAILY_AR_COUNT = s.D_AR, DAILY_ACTIVE = s.D_ACT, DAILY_CALL_SCORE_SUM = s.D_CS, DAILY_CALL_COUNT = s.D_CC,
                    DAILY_RET_CREATED = s.D_RC, DAILY_RET_MET = s.D_RM,
                    RENEWAL_M1_M2M = s.M1M, RENEWAL_M1_M2M_CALLED = s.M1MC,
                    RENEWAL_M2_M2M = s.M2M, RENEWAL_M2_M2M_CALLED = s.M2MC,
                    RENEWAL_M3_M2M = s.M3M, RENEWAL_M3_M2M_CALLED = s.M3MC
            """)
        print(f"✓ {len(csms)} CSMs")

    print(f"\nDone. {len(dates_to_fill)} dates backfilled.")
    conn.close()


if __name__ == "__main__":
    main()
