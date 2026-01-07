from .data_fetcher_fundamentals_helpers import av_value, safe_div, statement_value_at


def build_metric_snapshot(
    metric_keys,
    income_df,
    balance_df,
    cashflow_df,
    col,
    tax_rate_info,
    employees,
    annualize_factor=1,
):
    if col is None:
        return {}
    snapshot = {key: None for key in metric_keys}

    revenue_q = statement_value_at(income_df, ("Total Revenue", "Revenue"), col)
    cogs_q = statement_value_at(income_df, ("Cost Of Revenue", "Cost Of Goods Sold"), col)
    gross_profit_q = statement_value_at(income_df, ("Gross Profit",), col)
    if gross_profit_q is None and revenue_q is not None and cogs_q is not None:
        gross_profit_q = revenue_q - cogs_q

    operating_income_q = statement_value_at(
        income_df, ("Operating Income", "Operating Income Loss"), col
    )
    ebitda_q = statement_value_at(income_df, ("EBITDA",), col)
    sga_q = statement_value_at(
        income_df,
        (
            "Selling General Administrative",
            "Selling General And Administrative",
            "Selling, General And Administrative",
            "Selling General Administrative Expense",
            "Selling General And Administrative Expense",
        ),
        col,
    )
    rd_q = statement_value_at(
        income_df,
        (
            "Research Development",
            "Research And Development",
            "Research Development Expense",
            "Research And Development Expense",
        ),
        col,
    )
    pretax_q = statement_value_at(
        income_df, ("Income Before Tax", "Pretax Income", "Income Before Taxes"), col
    )
    tax_expense_q = statement_value_at(
        income_df, ("Income Tax Expense", "Provision For Income Taxes", "Tax Provision"), col
    )
    net_income_q = statement_value_at(
        income_df,
        (
            "Net Income",
            "Net Income Applicable To Common Shares",
            "Net Income Common Stockholders",
        ),
        col,
    )

    total_assets_q = statement_value_at(balance_df, ("Total Assets",), col)
    current_liabilities_q = statement_value_at(
        balance_df, ("Total Current Liabilities", "Current Liabilities"), col
    )
    total_equity_q = statement_value_at(
        balance_df, ("Total Stockholder Equity", "Total Stockholders Equity", "Total Equity"), col
    )
    total_debt_q = statement_value_at(balance_df, ("Total Debt",), col)
    if total_debt_q is None:
        short_debt_q = statement_value_at(
            balance_df, ("Short Long Term Debt", "Short Term Debt", "Short/Long Term Debt"), col
        )
        long_debt_q = statement_value_at(
            balance_df,
            (
                "Long Term Debt",
                "Long-Term Debt",
                "Long Term Debt And Capital Lease Obligation",
            ),
            col,
        )
        if short_debt_q is not None or long_debt_q is not None:
            total_debt_q = (short_debt_q or 0) + (long_debt_q or 0)
    cash_q = statement_value_at(
        balance_df,
        (
            "Cash And Cash Equivalents",
            "Cash And Cash Equivalents And Short Term Investments",
            "Cash",
        ),
        col,
    )

    capex_q = statement_value_at(cashflow_df, ("Capital Expenditures", "Capital Expenditure"), col)
    operating_cf_q = statement_value_at(
        cashflow_df, ("Total Cash From Operating Activities", "Operating Cash Flow"), col
    )

    if annualize_factor not in (None, 0, 1):
        revenue_q = revenue_q * annualize_factor if revenue_q is not None else None
        cogs_q = cogs_q * annualize_factor if cogs_q is not None else None
        gross_profit_q = gross_profit_q * annualize_factor if gross_profit_q is not None else None
        operating_income_q = (
            operating_income_q * annualize_factor if operating_income_q is not None else None
        )
        ebitda_q = ebitda_q * annualize_factor if ebitda_q is not None else None
        sga_q = sga_q * annualize_factor if sga_q is not None else None
        rd_q = rd_q * annualize_factor if rd_q is not None else None
        pretax_q = pretax_q * annualize_factor if pretax_q is not None else None
        tax_expense_q = tax_expense_q * annualize_factor if tax_expense_q is not None else None
        net_income_q = net_income_q * annualize_factor if net_income_q is not None else None
        capex_q = capex_q * annualize_factor if capex_q is not None else None
        operating_cf_q = (
            operating_cf_q * annualize_factor if operating_cf_q is not None else None
        )

    fcf_q = None
    if operating_cf_q is not None and capex_q is not None:
        fcf_q = operating_cf_q - abs(capex_q)

    tax_rate_q = None
    if pretax_q not in (None, 0) and tax_expense_q is not None:
        tax_rate_q = tax_expense_q / pretax_q
    elif operating_income_q not in (None, 0) and tax_expense_q is not None:
        tax_rate_q = tax_expense_q / operating_income_q
    if tax_rate_q is None:
        tax_rate_q = tax_rate_info
    if tax_rate_q is not None and (tax_rate_q < 0 or tax_rate_q > 1):
        tax_rate_q = None

    profit_q = operating_income_q if operating_income_q is not None else ebitda_q
    if profit_q is None:
        profit_q = pretax_q
    nopat_q = None
    if profit_q is not None and tax_rate_q is not None:
        nopat_q = profit_q * (1 - tax_rate_q)

    invested_capital_q = None
    if total_debt_q is not None and total_equity_q is not None:
        invested_capital_q = total_debt_q + total_equity_q
        if cash_q is not None:
            invested_capital_q -= cash_q
    if (
        invested_capital_q is None
        and total_assets_q is not None
        and current_liabilities_q is not None
    ):
        invested_capital_q = total_assets_q - current_liabilities_q

    snapshot["revenuePerEmployee"] = safe_div(revenue_q, employees)
    snapshot["grossProfitPerEmployee"] = safe_div(gross_profit_q, employees)
    snapshot["operatingIncomePerEmployee"] = safe_div(operating_income_q or ebitda_q, employees)
    snapshot["sgaPerEmployee"] = safe_div(sga_q, employees)
    snapshot["salesPerSalesperson"] = None
    snapshot["roic"] = safe_div(nopat_q, invested_capital_q)
    snapshot["roa"] = safe_div(net_income_q, total_assets_q)
    snapshot["assetTurnover"] = safe_div(revenue_q, total_assets_q)
    snapshot["capexIntensity"] = safe_div(
        abs(capex_q) if capex_q is not None else None, revenue_q
    )
    snapshot["freeCashFlowMargin"] = safe_div(fcf_q, revenue_q)
    snapshot["grossMargin"] = safe_div(gross_profit_q, revenue_q)
    snapshot["operatingMargin"] = safe_div(operating_income_q, revenue_q)
    snapshot["sgaPercentRevenue"] = safe_div(sga_q, revenue_q)
    snapshot["rdPercentRevenue"] = safe_div(rd_q, revenue_q)

    return snapshot


def build_metric_snapshot_av(
    metric_keys,
    income_report,
    balance_report,
    cashflow_report,
    tax_rate_info,
    employees,
    annualize_factor=1,
):
    snapshot = {key: None for key in metric_keys}

    revenue_q = av_value(income_report, ("totalRevenue",))
    cogs_q = av_value(income_report, ("costOfRevenue",))
    gross_profit_q = av_value(income_report, ("grossProfit",))
    if gross_profit_q is None and revenue_q is not None and cogs_q is not None:
        gross_profit_q = revenue_q - cogs_q

    operating_income_q = av_value(income_report, ("operatingIncome",))
    ebitda_q = av_value(income_report, ("ebitda",))
    sga_q = av_value(
        income_report,
        (
            "sellingGeneralAdministrative",
            "sellingGeneralAndAdministrative",
            "sellingGeneralAdministrativeExpense",
        ),
    )
    rd_q = av_value(
        income_report,
        ("researchAndDevelopment", "researchDevelopment", "researchAndDevelopmentExpense"),
    )
    pretax_q = av_value(income_report, ("incomeBeforeTax",))
    tax_expense_q = av_value(income_report, ("incomeTaxExpense",))
    net_income_q = av_value(income_report, ("netIncome",))

    total_assets_q = av_value(balance_report, ("totalAssets",))
    current_liabilities_q = av_value(balance_report, ("totalCurrentLiabilities",))
    total_equity_q = av_value(
        balance_report,
        ("totalShareholderEquity", "totalStockholderEquity", "totalStockholdersEquity"),
    )
    total_debt_q = av_value(
        balance_report, ("shortLongTermDebtTotal", "totalDebt", "shortTermDebt", "longTermDebt")
    )
    cash_q = av_value(
        balance_report,
        (
            "cashAndCashEquivalentsAtCarryingValue",
            "cashAndCashEquivalents",
            "cash",
        ),
    )

    capex_q = av_value(cashflow_report, ("capitalExpenditures",))
    operating_cf_q = av_value(cashflow_report, ("operatingCashflow",))

    if annualize_factor not in (None, 0, 1):
        revenue_q = revenue_q * annualize_factor if revenue_q is not None else None
        cogs_q = cogs_q * annualize_factor if cogs_q is not None else None
        gross_profit_q = gross_profit_q * annualize_factor if gross_profit_q is not None else None
        operating_income_q = (
            operating_income_q * annualize_factor if operating_income_q is not None else None
        )
        ebitda_q = ebitda_q * annualize_factor if ebitda_q is not None else None
        sga_q = sga_q * annualize_factor if sga_q is not None else None
        rd_q = rd_q * annualize_factor if rd_q is not None else None
        pretax_q = pretax_q * annualize_factor if pretax_q is not None else None
        tax_expense_q = tax_expense_q * annualize_factor if tax_expense_q is not None else None
        net_income_q = net_income_q * annualize_factor if net_income_q is not None else None
        capex_q = capex_q * annualize_factor if capex_q is not None else None
        operating_cf_q = (
            operating_cf_q * annualize_factor if operating_cf_q is not None else None
        )

    fcf_q = None
    if operating_cf_q is not None and capex_q is not None:
        fcf_q = operating_cf_q - abs(capex_q)

    tax_rate_q = None
    if pretax_q not in (None, 0) and tax_expense_q is not None:
        tax_rate_q = tax_expense_q / pretax_q
    elif operating_income_q not in (None, 0) and tax_expense_q is not None:
        tax_rate_q = tax_expense_q / operating_income_q
    if tax_rate_q is None:
        tax_rate_q = tax_rate_info
    if tax_rate_q is not None and (tax_rate_q < 0 or tax_rate_q > 1):
        tax_rate_q = None

    profit_q = operating_income_q if operating_income_q is not None else ebitda_q
    if profit_q is None:
        profit_q = pretax_q
    nopat_q = None
    if profit_q is not None and tax_rate_q is not None:
        nopat_q = profit_q * (1 - tax_rate_q)

    invested_capital_q = None
    if total_debt_q is not None and total_equity_q is not None:
        invested_capital_q = total_debt_q + total_equity_q
        if cash_q is not None:
            invested_capital_q -= cash_q
    if (
        invested_capital_q is None
        and total_assets_q is not None
        and current_liabilities_q is not None
    ):
        invested_capital_q = total_assets_q - current_liabilities_q

    snapshot["revenuePerEmployee"] = safe_div(revenue_q, employees)
    snapshot["grossProfitPerEmployee"] = safe_div(gross_profit_q, employees)
    snapshot["operatingIncomePerEmployee"] = safe_div(operating_income_q or ebitda_q, employees)
    snapshot["sgaPerEmployee"] = safe_div(sga_q, employees)
    snapshot["salesPerSalesperson"] = None
    snapshot["roic"] = safe_div(nopat_q, invested_capital_q)
    snapshot["roa"] = safe_div(net_income_q, total_assets_q)
    snapshot["assetTurnover"] = safe_div(revenue_q, total_assets_q)
    snapshot["capexIntensity"] = safe_div(
        abs(capex_q) if capex_q is not None else None, revenue_q
    )
    snapshot["freeCashFlowMargin"] = safe_div(fcf_q, revenue_q)
    snapshot["grossMargin"] = safe_div(gross_profit_q, revenue_q)
    snapshot["operatingMargin"] = safe_div(operating_income_q, revenue_q)
    snapshot["sgaPercentRevenue"] = safe_div(sga_q, revenue_q)
    snapshot["rdPercentRevenue"] = safe_div(rd_q, revenue_q)

    return snapshot
