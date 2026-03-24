# CS Performance Hub — Project Context

## What This Is
A Customer Success performance dashboard for Podium, inspired by Bill Walsh's "The Score Takes Care of Itself" — if CSMs master the fundamentals (quality calls + frequency + right customers), churn takes care of itself. Built for COO Chance and the CS leadership team.

## Architecture
- **Dashboard**: `docs/index.html` — single-file vanilla JS + CSS (~4500 lines), served via GitHub Pages
- **Next.js version**: `podium-internal-tools/cs-performance-hub` — Next.js 16 wrapper, deployed to cs-performance-hub.podium-tools.com via AWS App Runner
- **Data**: `docs/cs_perf_data.json` — exported from Snowflake, contains all CSM metrics + sparklines + risk detail + renewal data
- **Export script**: `~/Documents/Development/sigma-mcp/export_cs_perf_data.py` — queries Snowflake, writes JSON
- **Sync script**: `scripts/sync-snowflake.sh` — runs export, outputs to docs/
- **Migration script**: `scripts/migrate_v2.py` — updates Snowflake procedure + tables (run once for schema changes)
- **Backfill script**: `scripts/backfill_snapshots.py` — reconstructs historical snapshots from source tables
- **Enrichment script**: `scripts/enrich_snapshots.py` — adds UFR/SLA raw counts to existing snapshots
- **Recompute script**: `scripts/recompute_snapshots.py` — updates call score, SLA, composite for methodology changes
- **Live URLs**: https://rookinfresh.github.io/cs-performance-hub/ (GitHub Pages) + https://cs-performance-hub.podium-tools.com/ (Podium internal)

## Snowflake Tables
- `ANALYST_SANDBOX.JOSH_ROOKSTOOL.CS_PERF_HUB_MASTER` — main CSM metrics table, rebuilt daily at 6 AM MST by `REFRESH_CS_PERF_HUB()` procedure via `REFRESH_CS_PERF_HUB_TASK`
- `ANALYST_SANDBOX.JOSH_ROOKSTOOL.CS_PERF_HUB_SNAPSHOTS` — daily snapshots for sparkline trends (SNAPSHOT_DATE, CSM_NAME, COMPOSITE_SCORE, ARS_PER_DAY, AVG_CALL_SCORE, RET_SLA_PCT, RET_SLA_DONE, RET_SLA_TOTAL, RENEWAL_COVERAGE_PCT, RENEWAL_M1_CALLED, RENEWAL_M1_TOTAL, RENEWAL_M2_CALLED, RENEWAL_M2_TOTAL, RENEWAL_M3_CALLED, RENEWAL_M3_TOTAL, OVERALL_STATUS). No pruning — rows accumulate indefinitely.
- `BUILD.SALESFORCE.CORE_CASES` — onboarding + retention cases
- `BUILD.CUSTOMER_SUCCESS.CORE_INTERACTION_INTELLIGENCE_PROCESSED` — AI risk model (Claude Sonnet 4.6 via Cortex), per-org risk/sentiment analysis
- `BUILD.SALESFORCE.CORE_USERS` — CSM allowlist (title-based, excludes managers/VPs/Australia)
- `ANALYSIS.FINANCE.SALESFORCE_ORGANIZATION_MONTH_MRR_PLUS_6_MONTHS` — org-level MRR, contract data, renewal cohorts
- Call scoring (ANALYSIS.CUSTOMER_SUCCESS): HVAC, AUTOMOTIVE (covers OEM + Auto), MEDSPA, ACCOUNT_REVIEW_GRADING_EMERGING_MARKETS (covers Emerging + Jewelry + FAM + Retail) — all 8 verticals covered

## Key Data Fields in JSON
```
csms[]: csm, vertical, manager, segment, director, orgCount, bookArr, locationCount,
        arsPerDay, callScore, retSLA, retSLADone, retCases, composite, compositeDelta7d,
        renewalCohortTotal, renewalCohortCalled, renewalCoveragePct, renewalM2mOrgs, renewalAnnualOrgs,
        openOnboardingCases, openRetentionCases, highRiskOrgCount, avgRiskScore,
        cancellationIntentCount, avgSentimentScore, arScoreNorm, callScoreNorm, slaScoreNorm,
        overallStatus, gatedBy10in14, hasUnattempted10in14

sparklines: { "CSM Name": [{ date, composite, ar, call, sla, status }] }  (65-day window in JSON export, unlimited in Snowflake)
riskDetail: { "CSM Name": { totalOrgsAnalyzed, lowRisk, elevatedRisk, highRisk, criticalRisk, avgOpenAsks, avgDaysOpen } }
renewalByMonth: { "May 2026": { "CSM Name": { total, called, pct, m2m, annual } } }
ufrByMonth: { "CSM Name": { "May 2026": { called, total, pct }, ... } }  (absolute month labels, used for UFR sparklines)
meta.ufrPacing: { bizDaysElapsed, totalBizDays, progressRatio }
```

## Composite Score v2 Formula
```
COMPOSITE = (AR_NORM + CALL_NORM + SLA_NORM + UFR_PACING_NORM) / active_pillars × 100
```
- **AR Velocity**: `min(arsPerDay / segment_ceiling, 1.0)` — segment ceilings: AI/SOA=4.0, P+P=2.5, Mid Market=3.0, Strategic=1.5
- **Call Quality**: `callScore / 100` (0-1 scale, L30 window)
- **Retention SLA**: `retSLA_pct` (0-1 scale) — 2 business day outreach window (skips weekends), grace period for cases < 2 business days old, any CSM outreach counts (not just assigned CSM), self-created cases excluded, any call duration
- **UFR Pacing**: `min(called / expected, 1.0)` where expected = `cohort × (biz_days_elapsed / active_window_biz_days)`
- P+P segment excluded from UFR pillar
- Pillars excluded when no data exists (dynamic denominator)
- 10-in-14 does NOT gate composite (informational only)

## UFR Pacing Model
- Active outreach window: 120 days to 60 days before renewal month (target completion by 60 days out)
- Contacts after 60-day mark still count toward coverage, but pacing targets use the active window
- `getPacingWindow(monthLabel)` computes window dates and business days dynamically per selected month
- Pacing color: ≥1.0 ratio = green, ≥0.7 = amber, <0.7 = red
- M2M exclusion toggle: uses `MONTH_START_CONTRACT_TYPE_BUCKET = 'Month-to-Month'` from Snowflake

## Color Thresholds (fixed, not percentile)
| Metric | Green | Amber | Red |
|--------|-------|-------|-----|
| Composite | ≥ 80 | ≥ 50 | < 50 |
| Call Score | ≥ 80 | ≥ 70 | < 70 |
| Ret SLA | ≥ 80% | ≥ 50% | < 50% |
| AR/Day | Segment P75+ | Segment P25-P75 | < Segment P25 |
| UFR | On/ahead pace | 70-100% pace | < 70% pace |

AR/Day segment thresholds:
| Segment | Green | Amber | Red |
|---------|-------|-------|-----|
| AI/SOA | ≥ 3.3 | ≥ 2.0 | < 2.0 |
| P+P | ≥ 2.0 | ≥ 1.0 | < 1.0 |
| Mid Market | ≥ 2.5 | ≥ 1.2 | < 1.2 |
| Strategic | ≥ 1.2 | ≥ 0.7 | < 0.7 |

## Manager Hierarchy (as of Mar 2026)
- **Alex Howe** (Director): manages Jonathan Boyer, Maria Lam (managers) + Cameron Tribe, Landen Marcroft, Tanner Overbay (CSMs)
- **Liam Golightley** (Director): manages Brock Bird, Derek Tracy, Javier Herrera (managers)
- **Chris Nielson** (Director): manages Chris Isham (manager) + Gabriel Beccari, Jacob Wendell, Mackenzie Green, Connor Maloney (CSMs)
- **Eddy Alvarado**: manages Manuel Estrada (manager)
- **Carter Matheson, Samantha Aucunas, Sarah Swindle**: managers (direct reports unclear at director level)
- Maria Lam was recently promoted to manager under Alex Howe
- Everyone rolls up to COO Chance

## Dashboard Features
- **Executive Overview**: 4 hero KPI cards (AR/Day, Call Score, Ret SLA, UFR Coverage) with top vertical + top CSM, clickable for stack-ranked popups
- **Vertical Overview**: cards ranked by composite with status counts (● green ● amber ● red), UFR coverage, clickable for drill-down
- **Vertical Performance**: horizontal bar comparison across verticals for all 4 metrics (sorted best-to-worst)
- **Manager Leaderboard**: table with pillar pills, UFR progress bars, team status counts
- **CSM Rankings**: full ranked table with sparklines, pacing, risk, colored left border for status
- **Team Drilldown**: click vertical/manager → table of CSMs with pillar details
- **CSM Profile Panel**: slide-over with book stats, pillar breakdown, sparklines, risk intelligence, open cases
- **Multi-select filters**: Manager, Vertical, Segment (checkbox dropdowns)
- **M2M Toggle**: exclude month-to-month contracts from UFR calculations
- **UFR Month Picker**: sticky pills for Apr-Jul 2026, dynamically recalculates pacing
- **Metric Ranking Popups**: click hero cards → ranked CSMs groupable by All/Vertical/Manager
- **Dark/Light mode**

## Data Refresh Workflow
1. Snowflake procedure runs daily at 6 AM MST (snapshots then rebuilds master table)
2. Run `bash scripts/sync-snowflake.sh` to export fresh JSON (requires Snowflake SSO)
3. OR generate JSON directly via Snowflake MCP queries + Python processing
4. Commit and push to GitHub for deployment

## Known Issues / Future Work
- M2M contract classification uses `MONTH_START_CONTRACT_TYPE_BUCKET` (correct), but called/uncalled split for M2M vs annual is estimated proportionally
- No outcome tracking (churn rate correlation with composite)
- MRR-weighted UFR pacing would be more accurate (high-value renewals should be prioritized)
- Okta auth integration for podium-tools.com deployment (App Runner service exists, needs auth middleware)
- OEM UFR is inflated by M2M contracts — Chief of Staff flagged this
- Holiday calendar not yet implemented for retention SLA business day calculation (weekends only for now)

## Design Language
- Anthropic/Claude-inspired: warm neutrals, typography-first, generous whitespace
- Inter font, 14px body, status colors (green/amber/red) only for functional meaning
- Pills for status/values, inline SVG sparklines
- Frosted glass topbar with backdrop-filter blur
