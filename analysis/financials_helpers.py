from collections import defaultdict
from decimal import Decimal, InvalidOperation


def compute_partial_year_reports(quarterly_reports):
    if not quarterly_reports:
        return []

    reports_with_dates = [
        rpt for rpt in quarterly_reports if rpt.get("fiscalDateEnding")
    ]
    if not reports_with_dates:
        return []

    latest_report = max(reports_with_dates, key=lambda r: r["fiscalDateEnding"])
    latest_date = latest_report.get("fiscalDateEnding", "")
    if len(latest_date) < 7:
        return []

    latest_year = latest_date[:4]
    latest_month = latest_date[5:7]
    latest_quarter = month_to_quarter(latest_month)
    if latest_quarter is None or latest_quarter == 0:
        return []

    reports_by_year = defaultdict(dict)
    for report in quarterly_reports:
        date_str = report.get("fiscalDateEnding")
        if not date_str or len(date_str) < 7:
            continue
        year = date_str[:4]
        quarter = month_to_quarter(date_str[5:7])
        if quarter is None:
            continue
        reports_by_year[year][quarter] = report

    years_to_build = [str(int(latest_year) - offset) for offset in range(3)]
    partial_sets = []

    for year in years_to_build:
        year_reports = reports_by_year.get(year)
        if not year_reports:
            continue

        selected = []
        for quarter in range(1, latest_quarter + 1):
            quarter_report = year_reports.get(quarter)
            if not quarter_report:
                selected = []
                break
            selected.append(quarter_report)

        if not selected:
            continue

        aggregated = aggregate_quarter_reports(selected)
        aggregated["fiscalDateEnding"] = f"{year}-YTD-Q{latest_quarter}"
        aggregated["quarterRange"] = f"Q1-Q{latest_quarter}"
        aggregated["quarterCount"] = latest_quarter
        aggregated["quartersIncluded"] = [f"Q{i}" for i in range(1, latest_quarter + 1)]
        aggregated["year"] = year
        partial_sets.append(aggregated)

    return sorted(partial_sets, key=lambda x: x["year"])


def compute_annual_from_quarters(quarterly_reports):
    if not quarterly_reports:
        return []

    reports_by_year = defaultdict(list)
    for report in quarterly_reports:
        date_str = report.get("fiscalDateEnding")
        if not date_str or len(date_str) < 7:
            continue
        year = date_str[:4]
        reports_by_year[year].append(report)

    annual_reports = []
    for year, reports in reports_by_year.items():
        reports_sorted = sorted(reports, key=lambda r: r.get("fiscalDateEnding", ""))
        if len(reports_sorted) < 4:
            continue
        last_four = reports_sorted[-4:]
        aggregated = aggregate_quarter_reports(last_four)
        last_date = max(r.get("fiscalDateEnding", "") for r in last_four)
        aggregated["fiscalDateEnding"] = last_date
        annual_reports.append(aggregated)

    return sorted(annual_reports, key=lambda r: r.get("fiscalDateEnding", ""), reverse=True)


def aggregate_quarter_reports(reports):
    totals = {}
    currency = None

    for report in reports:
        if not currency:
            currency = report.get("reportedCurrency")
        for key, value in report.items():
            if key in {"fiscalDateEnding", "reportedCurrency"}:
                continue
            numeric_value = safe_decimal(value)
            if numeric_value is None:
                continue
            totals[key] = totals.get(key, Decimal("0")) + numeric_value

    aggregated = {key: decimal_to_string(val) for key, val in totals.items()}
    if currency:
        aggregated["reportedCurrency"] = currency
    return aggregated


def month_to_quarter(month_str):
    try:
        month = int(month_str)
    except (TypeError, ValueError):
        return None
    if 1 <= month <= 3:
        return 1
    if 4 <= month <= 6:
        return 2
    if 7 <= month <= 9:
        return 3
    if 10 <= month <= 12:
        return 4
    return None


def safe_decimal(value):
    if value in (None, "", "None"):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def decimal_to_string(value):
    if value == value.to_integral():
        return str(int(value))
    return format(value, "f")
