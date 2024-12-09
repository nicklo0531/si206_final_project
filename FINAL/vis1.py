import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
# conn = sqlite3.connect(os.path.abspath('final.db'))
conn = sqlite3.connect('final.db')
cur = conn.cursor()

joined_query = "SELECT wp.*, we.index_value, s.ticker FROM WeeklyPrices wp JOIN WeeklyEconomicIndex we ON wp.date = we.date join Stocks s on wp.stock_id = s.id  "
weekly_prices_query = "SELECT * FROM WeeklyPrices"
sentiment_query = "SELECT * FROM SentimentData"
weekly_economic_index = "SELECT * FROM WeeklyEconomicIndex"


joined_df = pd.read_sql(joined_query, conn)
weekly_prices_df = pd.read_sql(weekly_prices_query, conn)
sentiment_df = pd.read_sql(sentiment_query, conn)

# filter the five companies
start_date = "2024-07-02"
end_date = "2024-12-07"
filtered_index_joined_df = joined_df[(joined_df['date'] >= start_date) & (joined_df['date'] <= end_date)]
filtered_index_joined_df['percentage_growth_index'] = filtered_index_joined_df['index_value'].pct_change() * 100

nvda_joined_df = joined_df[joined_df['ticker'] == 'NVDA']
nvda_joined_df['percentage_growth_stock'] = round(((nvda_joined_df['close'] - nvda_joined_df['open']) / nvda_joined_df['open']) * 100, 2)
nvda_joined_df['percentage_growth_index'] = nvda_joined_df['index_value'].pct_change() * 100
filtered_nvda_joined_df = nvda_joined_df[(nvda_joined_df['date'] >= start_date) & (nvda_joined_df['date'] <= end_date)]

aapl_joined_df = joined_df[joined_df['ticker'] == 'AAPL']
aapl_joined_df['percentage_growth_stock'] = round(((aapl_joined_df['close'] - aapl_joined_df['open']) / aapl_joined_df['open']) * 100, 2)
aapl_joined_df['percentage_growth_index'] = aapl_joined_df['index_value'].pct_change() * 100
filtered_aapl_joined_df = aapl_joined_df[(aapl_joined_df['date'] >= start_date) & (aapl_joined_df['date'] <= end_date)]

msft_joined_df = joined_df[joined_df['ticker'] == 'MSFT']
msft_joined_df['percentage_growth_stock'] = round(((msft_joined_df['close'] - msft_joined_df['open']) / msft_joined_df['open']) * 100, 2)
msft_joined_df['percentage_growth_index'] = msft_joined_df['index_value'].pct_change() * 100
filtered_msft_joined_df = msft_joined_df[(msft_joined_df['date'] >= start_date) & (msft_joined_df['date'] <= end_date)]

meta_joined_df = joined_df[joined_df['ticker'] == 'META']
meta_joined_df['percentage_growth_stock'] = round(((meta_joined_df['close'] - meta_joined_df['open']) / meta_joined_df['open']) * 100, 2)
meta_joined_df['percentage_growth_index'] = meta_joined_df['index_value'].pct_change() * 100
filtered_meta_joined_df = meta_joined_df[(meta_joined_df['date'] >= start_date) & (meta_joined_df['date'] <= end_date)]

amzn_joined_df = joined_df[joined_df['ticker'] == 'AMZN']
amzn_joined_df['percentage_growth_stock'] = round(((amzn_joined_df['close'] - amzn_joined_df['open']) / amzn_joined_df['open']) * 100, 2)
amzn_joined_df['percentage_growth_index'] = amzn_joined_df['index_value'].pct_change() * 100
filtered_amzn_joined_df = amzn_joined_df[(amzn_joined_df['date'] >= start_date) & (amzn_joined_df['date'] <= end_date)]

# df dict
df_dict = {'NVDA': filtered_nvda_joined_df, 'AAPL': filtered_aapl_joined_df, 'MSFT': filtered_msft_joined_df, 'META': filtered_meta_joined_df, 'AMZN': filtered_amzn_joined_df}


# company_symbol: nvda, aapl, msft, amzn, meta
def graph_index_stock(company_symbol):
    if company_symbol in df_dict:
        df = df_dict[company_symbol]
        plt.plot(df['date'], df['percentage_growth_index'], color='red')
        plt.plot(df['date'], df['percentage_growth_stock'])
        plt.legend(['index', company_symbol])



def main (): 
    graph_index_stock('aapl')

if __name__ == '__main__':
    main()