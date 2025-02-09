import finnhub
finnhub_client = finnhub.Client(api_key="cuim17hr01qtqfmj0lt0cuim17hr01qtqfmj0ltg")

print(finnhub_client.company_peers('nvda'))