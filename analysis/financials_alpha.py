def is_alpha_vantage_error(payload) -> bool:
    if not isinstance(payload, dict):
        return True
    return any(key in payload for key in ("Note", "Error Message", "Information"))


def has_financial_reports(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    annual = payload.get("annualReports") or []
    quarterly = payload.get("quarterlyReports") or []
    partial = payload.get("partialYearReports") or []
    return bool(annual or quarterly or partial)
