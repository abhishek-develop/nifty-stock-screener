"""
Institutional-Grade Fundamental Scoring Engine for Indian Equities.
===================================================================
Features:
- 0 to 100 normalized business quality score independent of technical indicators.
- Sector-relative percentile ranking (e.g. IT vs IT, Banks vs Banks).
- Proportional weight redistribution when metrics are missing (no unearned 0s).
- 7 core categories (Profitability 25%, Growth 25%, Financial Strength 20%, Valuation 10%, Cash Flow 10%, Management 5%, Stability 5%).
- Explicit Bonus rules (cap +20) and Penalty rules (cap -30).
- 6-tier color-coded categorization (Dark Green, Green, Light Green, Yellow, Orange, Red).
- Full detailed score breakdown & transparency for every stock.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import math
import numpy as np


@dataclass
class ScoringConfig:
    """Configurable weights and thresholds for the scoring engine."""
    category_weights: Dict[str, float] = field(default_factory=lambda: {
        "profitability": 25.0,
        "growth": 25.0,
        "financial_strength": 20.0,
        "valuation": 10.0,
        "cash_flow": 10.0,
        "management": 5.0,
        "stability": 5.0
    })

    # Sub-metric internal weights per category
    profitability_subweights: Dict[str, float] = field(default_factory=lambda: {
        "roe": 0.30,
        "roce": 0.30,
        "operating_margin": 0.20,
        "net_margin": 0.10,
        "ebit_margin": 0.10
    })

    growth_subweights: Dict[str, float] = field(default_factory=lambda: {
        "sales_cagr_3y": 0.15,
        "sales_cagr_5y": 0.15,
        "eps_cagr_3y": 0.15,
        "eps_cagr_5y": 0.15,
        "profit_cagr_3y": 0.10,
        "profit_cagr_5y": 0.10,
        "quarterly_sales_yoy": 0.10,
        "quarterly_profit_yoy": 0.10
    })

    financial_strength_subweights: Dict[str, float] = field(default_factory=lambda: {
        "debt_to_equity": 0.25,        # Lower is better
        "interest_coverage": 0.25,    # Higher is better
        "current_ratio": 0.20,       # Ideal ~ 2.0
        "quick_ratio": 0.10,
        "cash_ratio": 0.05,
        "net_debt": 0.05,
        "debt_reduction_trend": 0.10
    })

    valuation_subweights: Dict[str, float] = field(default_factory=lambda: {
        "industry_relative_pe": 0.30,  # Lower vs industry is better
        "peg_ratio": 0.30,             # Ideal ~ 1.0
        "ev_ebitda": 0.20,             # Lower is better
        "price_to_book": 0.10,
        "price_to_sales": 0.10
    })

    cash_flow_subweights: Dict[str, float] = field(default_factory=lambda: {
        "operating_cf_growth": 0.25,
        "free_cash_flow": 0.25,
        "fcf_margin": 0.25,
        "ocf_to_net_profit": 0.25
    })

    management_subweights: Dict[str, float] = field(default_factory=lambda: {
        "promoter_holding": 0.40,
        "promoter_holding_trend": 0.20,
        "promoter_pledge": 0.20,       # Inverted (lower better)
        "institutional_holding_trend": 0.20
    })

    stability_subweights: Dict[str, float] = field(default_factory=lambda: {
        "sales_consistency": 0.30,
        "eps_consistency": 0.30,
        "roe_consistency": 0.20,
        "margin_consistency": 0.20
    })

    max_bonus: float = 20.0
    max_penalty: float = 30.0


def compute_percentile(val: float, arr: List[float], lower_is_better: bool = False) -> float:
    """Compute percentile rank (0 to 100) of val relative to arr."""
    valid_arr = [x for x in arr if x is not None and not math.isnan(x)]
    if not valid_arr:
        return 50.0
    if len(valid_arr) == 1:
        return 75.0 if val >= valid_arr[0] else 25.0

    count_below = sum(1 for x in valid_arr if (val > x if not lower_is_better else val < x))
    count_equal = sum(1 for x in valid_arr if val == x)
    pct = ((count_below + 0.5 * count_equal) / len(valid_arr)) * 100.0
    return float(np.clip(pct, 0.0, 100.0))


def compute_proximity_score(val: float, ideal_val: float, max_diff: float) -> float:
    """Compute proximity score (0 to 100) based on distance to ideal target."""
    if val is None or math.isnan(val):
        return 50.0
    diff = abs(val - ideal_val)
    score = 100.0 * max(0.0, (1.0 - (diff / max_diff)))
    return float(np.clip(score, 0.0, 100.0))


class InstitutionalFundamentalScoringEngine:
    """Core scoring orchestrator that normalizes metrics across sectors and computes fundamental scores."""

    def __init__(self, config: Optional[ScoringConfig] = None):
        self.config = config or ScoringConfig()

    def _extract_metric_values(self, stocks: List[Dict[str, Any]], key: str) -> List[float]:
        vals = []
        for s in stocks:
            v = s.get(key)
            if v is not None and not math.isnan(float(v)):
                vals.append(float(v))
        return vals

    def evaluate_universe(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Evaluate fundamental scores for all stocks in a universe with sector-relative percentiles."""
        if not stocks:
            return []

        # 1. Group stocks by sector for sector-relative ranking
        sector_groups: Dict[str, List[Dict[str, Any]]] = {}
        for s in stocks:
            sec = s.get("sector") or "General"
            sector_groups.setdefault(sec, []).append(s)

        # 2. Extract sector metric arrays
        sector_metric_pools: Dict[str, Dict[str, List[float]]] = {}
        for sec, sec_stocks in sector_groups.items():
            pool = {}
            for metric in [
                "roe", "roce", "operating_margin", "net_margin", "ebit_margin",
                "sales_cagr_3y", "sales_cagr_5y", "eps_cagr_3y", "eps_cagr_5y",
                "profit_cagr_3y", "profit_cagr_5y", "quarterly_sales_yoy", "quarterly_profit_yoy",
                "debt_to_equity", "interest_coverage", "current_ratio", "quick_ratio", "cash_ratio",
                "pe", "peg", "ev_ebitda", "pb", "ps", "industry_pe",
                "operating_cf_growth", "free_cash_flow", "fcf_margin", "ocf_to_net_profit",
                "promoter_holding", "promoter_holding_trend", "promoter_pledge", "institutional_holding_trend"
            ]:
                pool[metric] = self._extract_metric_values(sec_stocks, metric)
            sector_metric_pools[sec] = pool

        # Global metric pools (fallback for small sectors)
        global_pools: Dict[str, List[float]] = {}
        for metric in sector_metric_pools[next(iter(sector_metric_pools.keys()))].keys():
            global_pools[metric] = self._extract_metric_values(stocks, metric)

        evaluated_results = []

        # 3. Calculate category scores for each stock
        for stock in stocks:
            sec = stock.get("sector") or "General"
            sec_pool = sector_metric_pools.get(sec, global_pools)

            # Category 1: Profitability (25%)
            prof_score, prof_pcts = self._score_profitability(stock, sec_pool, global_pools)

            # Category 2: Growth (25%)
            growth_score, growth_pcts = self._score_growth(stock, sec_pool, global_pools)

            # Category 3: Financial Strength (20%)
            fin_score, fin_pcts = self._score_financial_strength(stock, sec_pool, global_pools)

            # Category 4: Valuation (10%)
            val_score, val_pcts = self._score_valuation(stock, sec_pool, global_pools)

            # Category 5: Cash Flow Quality (10%)
            cf_score, cf_pcts = self._score_cash_flow(stock, sec_pool, global_pools)

            # Category 6: Management & Governance (5%)
            mgmt_score, mgmt_pcts = self._score_management(stock, sec_pool, global_pools)

            # Category 7: Earnings Stability (5%)
            stab_score, stab_pcts = self._score_stability(stock)

            # Weighted Base Score
            cw = self.config.category_weights
            base_score = (
                prof_score * (cw["profitability"] / 100.0) +
                growth_score * (cw["growth"] / 100.0) +
                fin_score * (cw["financial_strength"] / 100.0) +
                val_score * (cw["valuation"] / 100.0) +
                cf_score * (cw["cash_flow"] / 100.0) +
                mgmt_score * (cw["management"] / 100.0) +
                stab_score * (cw["stability"] / 100.0)
            )

            # Bonuses and Penalties
            bonus, bonus_reasons = self._calculate_bonuses(stock)
            penalty, penalty_reasons = self._calculate_penalties(stock)

            # Final Score Calculation: Base + Bonus - Penalty (Clamped [0, 100])
            raw_final = base_score + bonus - penalty
            final_score = int(round(np.clip(raw_final, 0.0, 100.0)))

            # Color & Tier Assignment
            tier, color = self._get_score_tier_and_color(final_score)

            eval_dict = dict(stock)
            eval_dict.update({
                "fundamental_score": final_score,
                "fundamental_tier": tier,
                "fundamental_color": color,
                "profitability_score": round(prof_score, 1),
                "growth_score": round(growth_score, 1),
                "financial_strength_score": round(fin_score, 1),
                "valuation_score": round(val_score, 1),
                "cash_flow_score": round(cf_score, 1),
                "management_score": round(mgmt_score, 1),
                "stability_score": round(stab_score, 1),
                "bonus": round(bonus, 1),
                "bonus_reasons": bonus_reasons,
                "penalty": round(penalty, 1),
                "penalty_reasons": penalty_reasons,
                "metric_percentiles": {**prof_pcts, **growth_pcts, **fin_pcts, **val_pcts, **cf_pcts, **mgmt_pcts, **stab_pcts}
            })
            evaluated_results.append(eval_dict)

        # 4. Rank stocks by fundamental score
        evaluated_results.sort(key=lambda x: x["fundamental_score"], reverse=True)
        for rank, res in enumerate(evaluated_results, 1):
            res["fundamental_rank"] = rank

        return evaluated_results

    def _score_category_with_redistribution(
        self,
        sub_scores: Dict[str, Optional[float]],
        sub_weights: Dict[str, float]
    ) -> Tuple[float, Dict[str, float]]:
        """Score category with proportional weight redistribution for missing metrics."""
        available_metrics = {k: v for k, v in sub_scores.items() if v is not None and not math.isnan(v)}
        if not available_metrics:
            return 50.0, {}

        total_weight = sum(sub_weights[k] for k in available_metrics.keys())
        category_score = 0.0
        pcts = {}

        for k, score_val in available_metrics.items():
            norm_weight = sub_weights[k] / total_weight
            category_score += score_val * norm_weight
            pcts[f"{k}_score"] = round(score_val, 1)

        return float(category_score), pcts

    def _get_metric_pool(self, sec_pool: Dict[str, List[float]], global_pool: Dict[str, List[float]], key: str) -> List[float]:
        arr = sec_pool.get(key, [])
        return arr if len(arr) >= 3 else global_pool.get(key, [])

    # === CATEGORY 1: PROFITABILITY (25%) ===
    def _score_profitability(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}
        for m in ["roe", "roce", "operating_margin", "net_margin", "ebit_margin"]:
            val = s.get(m)
            if val is not None:
                pool = self._get_metric_pool(sec_pool, global_pool, m)
                sub_scores[m] = compute_percentile(float(val), pool, lower_is_better=False)
            else:
                sub_scores[m] = None
        return self._score_category_with_redistribution(sub_scores, self.config.profitability_subweights)

    # === CATEGORY 2: GROWTH (25%) ===
    def _score_growth(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}
        for m in ["sales_cagr_3y", "sales_cagr_5y", "eps_cagr_3y", "eps_cagr_5y", "profit_cagr_3y", "profit_cagr_5y", "quarterly_sales_yoy", "quarterly_profit_yoy"]:
            val = s.get(m)
            if val is not None:
                pool = self._get_metric_pool(sec_pool, global_pool, m)
                sub_scores[m] = compute_percentile(float(val), pool, lower_is_better=False)
            else:
                sub_scores[m] = None
        return self._score_category_with_redistribution(sub_scores, self.config.growth_subweights)

    # === CATEGORY 3: FINANCIAL STRENGTH (20%) ===
    def _score_financial_strength(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}

        # Debt to Equity (Lower better)
        de = s.get("debt_to_equity")
        sub_scores["debt_to_equity"] = compute_percentile(float(de), self._get_metric_pool(sec_pool, global_pool, "debt_to_equity"), lower_is_better=True) if de is not None else None

        # Interest Coverage (Higher better)
        ic = s.get("interest_coverage")
        sub_scores["interest_coverage"] = compute_percentile(float(ic), self._get_metric_pool(sec_pool, global_pool, "interest_coverage"), lower_is_better=False) if ic is not None else None

        # Current Ratio (Ideal ~ 2.0)
        cr = s.get("current_ratio")
        sub_scores["current_ratio"] = compute_proximity_score(float(cr), ideal_val=2.0, max_diff=2.0) if cr is not None else None

        # Quick Ratio & Cash Ratio
        qr = s.get("quick_ratio")
        sub_scores["quick_ratio"] = compute_percentile(float(qr), self._get_metric_pool(sec_pool, global_pool, "quick_ratio"), lower_is_better=False) if qr is not None else None

        cash_r = s.get("cash_ratio")
        sub_scores["cash_ratio"] = compute_percentile(float(cash_r), self._get_metric_pool(sec_pool, global_pool, "cash_ratio"), lower_is_better=False) if cash_r is not None else None

        # Net Debt & Reduction Trend
        nd = s.get("net_debt")
        sub_scores["net_debt"] = compute_percentile(float(nd), self._get_metric_pool(sec_pool, global_pool, "net_debt"), lower_is_better=True) if nd is not None else None

        d_trend = s.get("debt_reduction_trend")
        sub_scores["debt_reduction_trend"] = 80.0 if d_trend is True else (20.0 if d_trend is False else None)

        return self._score_category_with_redistribution(sub_scores, self.config.financial_strength_subweights)

    # === CATEGORY 4: VALUATION (10%) ===
    def _score_valuation(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}

        # Industry Relative PE (Company PE vs Industry PE)
        pe = s.get("pe")
        ind_pe = s.get("industry_pe")
        if pe is not None and pe > 0:
            if ind_pe and ind_pe > 0:
                rel_ratio = pe / ind_pe
                sub_scores["industry_relative_pe"] = float(np.clip(100.0 * max(0.0, (1.8 - rel_ratio) / 1.5), 0.0, 100.0))
            else:
                sub_scores["industry_relative_pe"] = compute_percentile(float(pe), self._get_metric_pool(sec_pool, global_pool, "pe"), lower_is_better=True)
        else:
            sub_scores["industry_relative_pe"] = None

        # PEG Ratio (Ideal ~ 1.0)
        peg = s.get("peg")
        sub_scores["peg_ratio"] = compute_proximity_score(float(peg), ideal_val=1.0, max_diff=2.5) if peg is not None and peg > 0 else None

        # EV/EBITDA, P/B, P/S (Lower better)
        for m in ["ev_ebitda", "price_to_book", "price_to_sales"]:
            v = s.get(m)
            sub_scores[m] = compute_percentile(float(v), self._get_metric_pool(sec_pool, global_pool, m), lower_is_better=True) if v is not None else None

        return self._score_category_with_redistribution(sub_scores, self.config.valuation_subweights)

    # === CATEGORY 5: CASH FLOW QUALITY (10%) ===
    def _score_cash_flow(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}
        for m in ["operating_cf_growth", "free_cash_flow", "fcf_margin", "ocf_to_net_profit"]:
            v = s.get(m)
            sub_scores[m] = compute_percentile(float(v), self._get_metric_pool(sec_pool, global_pool, m), lower_is_better=False) if v is not None else None
        return self._score_category_with_redistribution(sub_scores, self.config.cash_flow_subweights)

    # === CATEGORY 6: MANAGEMENT & GOVERNANCE (5%) ===
    def _score_management(self, s: Dict, sec_pool: Dict, global_pool: Dict) -> Tuple[float, Dict]:
        sub_scores = {}

        ph = s.get("promoter_holding")
        sub_scores["promoter_holding"] = compute_percentile(float(ph), self._get_metric_pool(sec_pool, global_pool, "promoter_holding"), lower_is_better=False) if ph is not None else None

        p_trend = s.get("promoter_holding_trend")
        sub_scores["promoter_holding_trend"] = 85.0 if (p_trend and p_trend > 0) else (30.0 if (p_trend and p_trend < 0) else 50.0) if p_trend is not None else None

        pp = s.get("promoter_pledge")
        sub_scores["promoter_pledge"] = float(np.clip(100.0 - (float(pp) * 2.5), 0.0, 100.0)) if pp is not None else 80.0

        inst_trend = s.get("institutional_holding_trend")
        sub_scores["institutional_holding_trend"] = 80.0 if (inst_trend and inst_trend > 0) else (30.0 if (inst_trend and inst_trend < 0) else 50.0) if inst_trend is not None else None

        return self._score_category_with_redistribution(sub_scores, self.config.management_subweights)

    # === CATEGORY 7: EARNINGS STABILITY (5%) ===
    def _score_stability(self, s: Dict) -> Tuple[float, Dict]:
        sub_scores = {}
        for m in ["sales_consistency", "eps_consistency", "roe_consistency", "margin_consistency"]:
            v = s.get(m)
            sub_scores[m] = float(v) if v is not None and 0 <= float(v) <= 100 else 60.0
        return self._score_category_with_redistribution(sub_scores, self.config.stability_subweights)

    # === BONUS CALCULATIONS (MAX +20 PTS) ===
    def _calculate_bonuses(self, s: Dict) -> Tuple[float, List[str]]:
        bonus = 0.0
        reasons = []

        de = s.get("debt_to_equity")
        if de is not None and float(de) == 0.0:
            bonus += 5.0
            reasons.append("Debt Free Company (+5)")

        roe = s.get("roe")
        if roe is not None and float(roe) >= 20.0:
            bonus += 5.0
            reasons.append("High ROE >= 20% (+5)")

        roce = s.get("roce")
        if roce is not None and float(roce) >= 20.0:
            bonus += 5.0
            reasons.append("High ROCE >= 20% (+5)")

        fcf = s.get("free_cash_flow")
        if fcf is not None and float(fcf) > 0.0:
            bonus += 5.0
            reasons.append("Positive Free Cash Flow (+5)")

        sales_cagr = s.get("sales_cagr_5y") or s.get("sales_cagr_3y")
        profit_cagr = s.get("profit_cagr_5y") or s.get("profit_cagr_3y")
        if sales_cagr and float(sales_cagr) >= 20.0 and profit_cagr and float(profit_cagr) >= 20.0:
            bonus += 5.0
            reasons.append("High Growth CAGR >= 20% (+5)")

        p_trend = s.get("promoter_holding_trend")
        if p_trend and float(p_trend) > 0:
            bonus += 2.0
            reasons.append("Increasing Promoter Holding (+2)")

        div_trend = s.get("dividend_growth_trend")
        if div_trend is True:
            bonus += 2.0
            reasons.append("Consistent Dividend Growth (+2)")

        final_bonus = float(min(self.config.max_bonus, bonus))
        return final_bonus, reasons

    # === PENALTY CALCULATIONS (MAX -30 PTS) ===
    def _calculate_penalties(self, s: Dict) -> Tuple[float, List[str]]:
        penalty = 0.0
        reasons = []

        de = s.get("debt_to_equity")
        if de is not None and float(de) > 150.0:
            penalty += 15.0
            reasons.append("High Debt to Equity > 150% (-15)")

        pp = s.get("promoter_pledge")
        if pp is not None and float(pp) > 25.0:
            penalty += 10.0
            reasons.append("Promoter Pledge > 25% (-10)")

        ocf = s.get("operating_cf_growth") or s.get("free_cash_flow")
        if ocf is not None and float(ocf) < 0.0:
            penalty += 15.0
            reasons.append("Negative Cash Flow (-15)")

        eps = s.get("eps")
        if eps is not None and float(eps) <= 0.0:
            penalty += 20.0
            reasons.append("Loss Making Company (-20)")

        ic = s.get("interest_coverage")
        if ic is not None and float(ic) < 2.0:
            penalty += 15.0
            reasons.append("Weak Interest Coverage < 2.0 (-15)")

        auditor_res = s.get("auditor_resignation")
        if auditor_res is True:
            penalty += 15.0
            reasons.append("Auditor Resignation Flag (-15)")

        final_penalty = float(min(self.config.max_penalty, penalty))
        return final_penalty, reasons

    def _get_score_tier_and_color(self, score: int) -> Tuple[str, str]:
        if score >= 90:
            return "Excellent Business", "#047857"  # Dark Green
        elif score >= 80:
            return "Very Strong", "#10b981"         # Green
        elif score >= 70:
            return "Good", "#34d399"               # Light Green
        elif score >= 60:
            return "Average", "#f59e0b"            # Yellow
        elif score >= 50:
            return "Weak", "#f97316"               # Orange
        else:
            return "Poor Fundamentals", "#ef4444"  # Red
