from .data_fetcher_fundamentals_helpers import (
    alpha_dates,
    alpha_report_lookup,
    alpha_report_map,
    alpha_report_map_by_period,
    latest_series_value,
    safe_div,
    safe_float,
    split_trends,
    statement_columns_by_recency,
    statement_float,
)
from .data_fetcher_fundamentals_metrics import build_metric_snapshot, build_metric_snapshot_av

_METRIC_KEYS = (
    "revenuePerEmployee",
    "grossProfitPerEmployee",
    "operatingIncomePerEmployee",
    "sgaPerEmployee",
    "salesPerSalesperson",
    "roic",
    "roa",
    "assetTurnover",
    "capexIntensity",
    "freeCashFlowMargin",
    "grossMargin",
    "operatingMargin",
    "sgaPercentRevenue",
    "rdPercentRevenue",
)

_EMPTY_KEYS = (
    "trailingPE",
    "forwardPE",
    "PEG",
    "PGI",
    "dividendYield",
    "beta",
    "marketCap",
    "revenuePerEmployee",
    "grossProfitPerEmployee",
    "operatingIncomePerEmployee",
    "sgaPerEmployee",
    "salesPerSalesperson",
    "roic",
    "roa",
    "assetTurnover",
    "capexIntensity",
    "freeCashFlowMargin",
    "grossMargin",
    "operatingMargin",
    "sgaPercentRevenue",
    "rdPercentRevenue",
)


def extract_fundamentals(info, fast_info, statements=None):
    income_stmt = (statements or {}).get("income")
    balance_sheet = (statements or {}).get("balance")
    cashflow = (statements or {}).get("cashflow")
    income_quarterly = (statements or {}).get("income_quarterly")
    balance_quarterly = (statements or {}).get("balance_quarterly")
    cashflow_quarterly = (statements or {}).get("cashflow_quarterly")
    alpha_financials = (statements or {}).get("alpha_financials") or {}

    def info_float(key, fallback_keys=None):
        raw = info.get(key)
        if raw is None and fallback_keys:
            for alt_key in fallback_keys:
                raw = info.get(alt_key)
                if raw is not None:
                    break
        return safe_float(raw)

    trailing_pe = info_float("trailingPE")
    forward_pe = info_float("forwardPE")
    trailing_eps = info_float("trailingEps")
    forward_eps = info_float("forwardEps")
    earnings_growth = info_float("earningsGrowth")
    current_price = (
        info_float("currentPrice", ["regularMarketPrice", "previousClose"])
        or safe_float(fast_info.get("lastPrice"))
        or safe_float(fast_info.get("regularMarketPrice"))
        or safe_float(fast_info.get("previousClose"))
    )

    if trailing_pe is None and trailing_eps is not None and current_price:
        trailing_pe = current_price / trailing_eps
    if forward_pe is None and forward_eps is not None and current_price:
        forward_pe = current_price / forward_eps

    if forward_pe is not None and earnings_growth is not None and earnings_growth != 0:
        peg = forward_pe / (earnings_growth * 100)
    else:
        peg = None

    if forward_pe is not None and trailing_pe is not None and trailing_pe != 0:
        pgi = forward_pe / trailing_pe
    elif trailing_eps is not None and forward_eps is not None and forward_eps != 0:
        pgi = trailing_eps / forward_eps
    else:
        pgi = None

    trailing_peg = info_float("trailingPegRatio")
    if peg is None:
        peg = trailing_peg
    dividend_yield = info_float("dividendYield")
    beta = info_float("beta")
    market_cap = info_float("marketCap")
    if market_cap is None:
        market_cap = safe_float(fast_info.get("marketCap"))
    shares_outstanding = info_float("sharesOutstanding")
    if market_cap is None and current_price and shares_outstanding:
        market_cap = current_price * shares_outstanding
    price_to_book = info_float("priceToBook")
    debt_to_equity = info_float("debtToEquity")

    employees = info_float("fullTimeEmployees")

    revenue = info_float("totalRevenue")
    if revenue is None:
        revenue = statement_float(income_stmt, ("Total Revenue", "Revenue"))

    cogs = info_float("costOfRevenue")
    if cogs is None:
        cogs = statement_float(income_stmt, ("Cost Of Revenue", "Cost Of Goods Sold"))

    gross_profit = info_float("grossProfits")
    if gross_profit is None:
        gross_profit = statement_float(income_stmt, ("Gross Profit",))
    if gross_profit is None and revenue is not None and cogs is not None:
        gross_profit = revenue - cogs

    operating_income = info_float("operatingIncome")
    if operating_income is None:
        operating_income = statement_float(income_stmt, ("Operating Income", "Operating Income Loss"))

    ebitda = info_float("ebitda")
    if ebitda is None:
        ebitda = statement_float(income_stmt, ("EBITDA",))

    sga = info_float("sellingGeneralAdministrative")
    if sga is None:
        sga = statement_float(
            income_stmt,
            (
                "Selling General Administrative",
                "Selling General And Administrative",
                "Selling, General And Administrative",
                "Selling General Administrative Expense",
                "Selling General And Administrative Expense",
            ),
        )

    rd = info_float("researchDevelopment")
    if rd is None:
        rd = statement_float(
            income_stmt,
            (
                "Research Development",
                "Research And Development",
                "Research Development Expense",
                "Research And Development Expense",
            ),
        )

    pretax_income = info_float("incomeBeforeTax", ["pretaxIncome"])
    if pretax_income is None:
        pretax_income = statement_float(
            income_stmt, ("Income Before Tax", "Pretax Income", "Income Before Taxes")
        )

    tax_expense = info_float("incomeTaxExpense")
    if tax_expense is None:
        tax_expense = statement_float(
            income_stmt, ("Income Tax Expense", "Provision For Income Taxes", "Tax Provision")
        )

    tax_rate = info_float("effectiveTaxRate", ["taxRateForCalcs"])
    if tax_rate is None and pretax_income not in (None, 0) and tax_expense is not None:
        tax_rate = tax_expense / pretax_income
    if tax_rate is None and operating_income not in (None, 0) and tax_expense is not None:
        tax_rate = tax_expense / operating_income
    if tax_rate is not None and (tax_rate < 0 or tax_rate > 1):
        tax_rate = None

    net_income = info_float("netIncomeToCommon", ["netIncome"])
    if net_income is None:
        net_income = statement_float(
            income_stmt,
            ("Net Income", "Net Income Applicable To Common Shares", "Net Income Common Stockholders"),
        )

    total_assets = info_float("totalAssets")
    if total_assets is None:
        total_assets = statement_float(balance_sheet, ("Total Assets",))

    current_liabilities = statement_float(
        balance_sheet,
        ("Total Current Liabilities", "Current Liabilities"),
    )

    total_equity = info_float("totalStockholderEquity", ["totalStockholdersEquity", "totalEquity"])
    if total_equity is None:
        total_equity = statement_float(
            balance_sheet,
            ("Total Stockholder Equity", "Total Stockholders Equity", "Total Equity"),
        )

    total_debt = info_float("totalDebt")
    if total_debt is None:
        total_debt = statement_float(balance_sheet, ("Total Debt",))
    if total_debt is None:
        short_debt = statement_float(
            balance_sheet,
            ("Short Long Term Debt", "Short Term Debt", "Short/Long Term Debt"),
        )
        long_debt = statement_float(
            balance_sheet,
            ("Long Term Debt", "Long-Term Debt", "Long Term Debt And Capital Lease Obligation"),
        )
        if short_debt is not None or long_debt is not None:
            total_debt = (short_debt or 0) + (long_debt or 0)

    cash = info_float("totalCash")
    if cash is None:
        cash = statement_float(
            balance_sheet,
            (
                "Cash And Cash Equivalents",
                "Cash And Cash Equivalents And Short Term Investments",
                "Cash",
            ),
        )

    capex = info_float("capitalExpenditures")
    if capex is None:
        capex = statement_float(cashflow, ("Capital Expenditures", "Capital Expenditure"))

    operating_cashflow = info_float("operatingCashflow")
    if operating_cashflow is None:
        operating_cashflow = statement_float(
            cashflow, ("Total Cash From Operating Activities", "Operating Cash Flow")
        )

    free_cash_flow = info_float("freeCashflow")
    if free_cash_flow is None:
        free_cash_flow = statement_float(cashflow, ("Free Cash Flow",))
    if free_cash_flow is None and operating_cashflow is not None and capex is not None:
        free_cash_flow = operating_cashflow - abs(capex)

    invested_capital = None
    if total_debt is not None and total_equity is not None:
        invested_capital = total_debt + total_equity
        if cash is not None:
            invested_capital -= cash
    if invested_capital is None and total_assets is not None and current_liabilities is not None:
        invested_capital = total_assets - current_liabilities

    nopat = None
    profit_base = operating_income if operating_income is not None else ebitda
    if profit_base is None:
        profit_base = pretax_income
    if profit_base is not None and tax_rate is not None:
        nopat = profit_base * (1 - tax_rate)

    revenue_per_employee = safe_div(revenue, employees)
    gross_profit_per_employee = safe_div(gross_profit, employees)
    operating_income_per_employee = safe_div(operating_income or ebitda, employees)
    sga_per_employee = safe_div(sga, employees)
    sales_per_salesperson = None

    roic = safe_div(nopat, invested_capital)
    roa = safe_div(net_income, total_assets)
    asset_turnover = safe_div(revenue, total_assets)
    capex_intensity = safe_div(abs(capex) if capex is not None else None, revenue)
    free_cash_flow_margin = safe_div(free_cash_flow, revenue)
    gross_margin = safe_div(gross_profit, revenue)
    operating_margin = safe_div(operating_income, revenue)
    sga_percent_revenue = safe_div(sga, revenue)
    rd_percent_revenue = safe_div(rd, revenue)

    metric_series = {}
    quarter_cols = statement_columns_by_recency(income_quarterly)[:8]

    def add_series(metric_key, value):
        metric_series.setdefault(metric_key, []).append(value)

    alpha_income = alpha_financials.get("income_statement") or {}
    alpha_balance = alpha_financials.get("balance_sheet") or {}
    alpha_cashflow = alpha_financials.get("cash_flow") or {}
    alpha_quarterly = alpha_income.get("quarterlyReports") or []
    alpha_balance_quarterly = alpha_balance.get("quarterlyReports") or []
    alpha_cashflow_quarterly = alpha_cashflow.get("quarterlyReports") or []

    alpha_quarter_dates = alpha_dates(alpha_quarterly)[:8]
    if alpha_quarter_dates:
        income_map = alpha_report_map(alpha_quarterly)
        balance_map = alpha_report_map(alpha_balance_quarterly)
        cashflow_map = alpha_report_map(alpha_cashflow_quarterly)
        income_period_map = alpha_report_map_by_period(alpha_quarterly)
        balance_period_map = alpha_report_map_by_period(alpha_balance_quarterly)
        cashflow_period_map = alpha_report_map_by_period(alpha_cashflow_quarterly)
        tax_rate_info = tax_rate
        for date in alpha_quarter_dates:
            income_report = alpha_report_lookup(income_period_map, income_map, date, annual=False)
            balance_report = alpha_report_lookup(balance_period_map, balance_map, date, annual=False)
            cashflow_report = alpha_report_lookup(cashflow_period_map, cashflow_map, date, annual=False)
            snapshot = build_metric_snapshot_av(
                _METRIC_KEYS,
                income_report,
                balance_report,
                cashflow_report,
                tax_rate_info,
                employees,
                annualize_factor=4,
            )
            for metric_key in _METRIC_KEYS:
                add_series(metric_key, snapshot.get(metric_key))

    if not alpha_quarter_dates and quarter_cols:
        tax_rate_info = tax_rate
        for col in quarter_cols:
            snapshot = build_metric_snapshot(
                _METRIC_KEYS,
                income_quarterly,
                balance_quarterly,
                cashflow_quarterly,
                col,
                tax_rate_info,
                employees,
                annualize_factor=4,
            )
            for metric_key in _METRIC_KEYS:
                add_series(metric_key, snapshot.get(metric_key))

    has_metric_series = any(metric_series.values())
    annual_cols = statement_columns_by_recency(income_stmt)
    if not alpha_quarter_dates and not quarter_cols and annual_cols and not has_metric_series:
        recent_col = annual_cols[0]
        prior_col = annual_cols[1] if len(annual_cols) > 1 else None
        recent_snapshot = build_metric_snapshot(
            _METRIC_KEYS,
            income_stmt,
            balance_sheet,
            cashflow,
            recent_col,
            tax_rate,
            employees,
        )
        prior_snapshot = build_metric_snapshot(
            _METRIC_KEYS,
            income_stmt,
            balance_sheet,
            cashflow,
            prior_col,
            tax_rate,
            employees,
        )
        for metric_key in _METRIC_KEYS:
            metric_series.setdefault(metric_key, [])
            metric_series[metric_key].extend([recent_snapshot.get(metric_key)] * 4)
            if prior_col is not None:
                metric_series[metric_key].extend([prior_snapshot.get(metric_key)] * 4)

    alpha_annual = alpha_income.get("annualReports") or []
    alpha_annual_dates = alpha_dates(alpha_annual)
    if not alpha_quarter_dates and alpha_annual_dates:
        income_map = alpha_report_map(alpha_annual)
        balance_map = alpha_report_map(alpha_balance.get("annualReports") or [])
        cashflow_map = alpha_report_map(alpha_cashflow.get("annualReports") or [])
        income_period_map = alpha_report_map_by_period(alpha_annual, annual=True)
        balance_period_map = alpha_report_map_by_period(
            alpha_balance.get("annualReports") or [],
            annual=True,
        )
        cashflow_period_map = alpha_report_map_by_period(
            alpha_cashflow.get("annualReports") or [],
            annual=True,
        )
        recent_date = alpha_annual_dates[0]
        prior_date = alpha_annual_dates[1] if len(alpha_annual_dates) > 1 else None
        recent_snapshot = build_metric_snapshot_av(
            _METRIC_KEYS,
            alpha_report_lookup(income_period_map, income_map, recent_date, annual=True),
            alpha_report_lookup(balance_period_map, balance_map, recent_date, annual=True),
            alpha_report_lookup(cashflow_period_map, cashflow_map, recent_date, annual=True),
            tax_rate,
            employees,
        )
        prior_snapshot = None
        if prior_date:
            prior_snapshot = build_metric_snapshot_av(
                _METRIC_KEYS,
                alpha_report_lookup(income_period_map, income_map, prior_date, annual=True),
                alpha_report_lookup(balance_period_map, balance_map, prior_date, annual=True),
                alpha_report_lookup(cashflow_period_map, cashflow_map, prior_date, annual=True),
                tax_rate,
                employees,
            )
        for metric_key in _METRIC_KEYS:
            series = metric_series.setdefault(metric_key, [])
            if not series:
                series.extend([recent_snapshot.get(metric_key)] * 4)
                if prior_snapshot is not None:
                    series.extend([prior_snapshot.get(metric_key)] * 4)

    if alpha_quarter_dates and len(alpha_quarter_dates) < 8 and alpha_annual_dates:
        income_map = alpha_report_map(alpha_annual)
        balance_map = alpha_report_map(alpha_balance.get("annualReports") or [])
        cashflow_map = alpha_report_map(alpha_cashflow.get("annualReports") or [])
        income_period_map = alpha_report_map_by_period(alpha_annual, annual=True)
        balance_period_map = alpha_report_map_by_period(
            alpha_balance.get("annualReports") or [],
            annual=True,
        )
        cashflow_period_map = alpha_report_map_by_period(
            alpha_cashflow.get("annualReports") or [],
            annual=True,
        )
        prior_date = alpha_annual_dates[1] if len(alpha_annual_dates) > 1 else alpha_annual_dates[0]
        prior_snapshot = build_metric_snapshot_av(
            _METRIC_KEYS,
            alpha_report_lookup(income_period_map, income_map, prior_date, annual=True),
            alpha_report_lookup(balance_period_map, balance_map, prior_date, annual=True),
            alpha_report_lookup(cashflow_period_map, cashflow_map, prior_date, annual=True),
            tax_rate,
            employees,
        )
        for metric_key in _METRIC_KEYS:
            series = metric_series.setdefault(metric_key, [])
            if len(series) < 8:
                series.extend([prior_snapshot.get(metric_key)] * (8 - len(series)))

    if not alpha_quarter_dates and quarter_cols and len(quarter_cols) < 8 and len(annual_cols) > 1:
        prior_col = annual_cols[1]
        prior_snapshot = build_metric_snapshot(
            _METRIC_KEYS,
            income_stmt,
            balance_sheet,
            cashflow,
            prior_col,
            tax_rate,
            employees,
        )
        for metric_key in _METRIC_KEYS:
            series = metric_series.setdefault(metric_key, [])
            if len(series) < 8:
                series.extend([prior_snapshot.get(metric_key)] * (8 - len(series)))

    metric_trends = {key: split_trends(series) for key, series in metric_series.items()}

    if roic is None:
        roic = latest_series_value(metric_series.get("roic") or [])

    return {
        "trailingPE": trailing_pe,
        "forwardPE": forward_pe,
        "PEG": peg,
        "PGI": pgi,
        "trailingPEG": trailing_peg,
        "dividendYield": dividend_yield,
        "beta": beta,
        "marketCap": market_cap,
        "priceToBook": price_to_book,
        "forwardEPS": forward_eps,
        "trailingEPS": trailing_eps,
        "debtToEquity": debt_to_equity,
        "revenuePerEmployee": revenue_per_employee,
        "grossProfitPerEmployee": gross_profit_per_employee,
        "operatingIncomePerEmployee": operating_income_per_employee,
        "sgaPerEmployee": sga_per_employee,
        "salesPerSalesperson": sales_per_salesperson,
        "roic": roic,
        "roa": roa,
        "assetTurnover": asset_turnover,
        "capexIntensity": capex_intensity,
        "freeCashFlowMargin": free_cash_flow_margin,
        "grossMargin": gross_margin,
        "operatingMargin": operating_margin,
        "sgaPercentRevenue": sga_percent_revenue,
        "rdPercentRevenue": rd_percent_revenue,
        "metricTrends": metric_trends,
    }


def is_empty_fundamentals(payload):
    if not payload:
        return True
    for key in _EMPTY_KEYS:
        if payload.get(key) is not None:
            return False
    return True
