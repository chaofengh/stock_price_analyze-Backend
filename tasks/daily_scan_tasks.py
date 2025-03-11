from analysis.daily_scan import daily_scan

# Store the latest scan result in a module-level variable.
latest_scan_result = None

def daily_scan_wrapper():
    """
    Run daily_scan and store the result for reference by any route.
    """
    global latest_scan_result
    latest_scan_result = daily_scan()
    print("daily_scan ran, updated latest_scan_result.")
