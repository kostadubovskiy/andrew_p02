
"""
This notebook calculates the "profit" per token for an account
Profit here is calculated using FIFO(first-in-first-out).
"""


# Install Libraries - if not importing libraries throws errors
# 


# Import Libraries
import csv
from shroomdk import ShroomDK
from shroomdk.errors import QueryRunExecutionError, ServerError, QueryRunTimeoutError, QueryRunRateLimitError, UserError, SDKError
import pandas as pd
import math
import requests
from datetime import date
from datetime import timedelta
from datetime import datetime
# !pip install pandas_datareader
import pandas_datareader as web
import datetime as dt
from dateutil import parser


# Metas

addresses = ["0x3471884f189fd7c63fe8c83601d28ce0cc1b3853"] # BLUR/LOOKS"0x3471884f189fd7c63fe8c83601d28ce0cc1b3853"]# (BLUR guy, 0x3471884f189fd7c63fe8c83601d28ce0cc1b3853), 0x86b575f63243a434c9a6d61b567d1a20e6c4bab3, 0xf3bed2bde0510ff5a058bc82cf0dcda28cc0dc42, 0x09029f5af07388127995ba31060fb314c5d79972, 0xb5e8add227067bf3db9989686285919fb669b580
days = 10000 # change this to the range you'd like. Alternatively change the start_date to a date string in the format commented
start_date = date.today() - timedelta(days=days)# '2023-02-01 12:00:00.000' # y-m-d 24hrt
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" # WETH 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 # USDC 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48 # WBTC 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599
BLUR = '0x5283d291dbcf85356a21ba090e6db59121208b44'
LOOKS = '0xf4d2888d29d722226fafa5d9b24f9164c092421e'
DC = '0x7B4328c127B85369D9f82ca0503B000D09CF9180'
where1_str = f""
for ind in range(len(addresses)):
  if ind < len(addresses) - 1:
    where1_str += f"'{addresses[ind]}',"
  else :
    where1_str += f"'{addresses[ind]}'"
  
tx_sql_pastNdays = f"""
    SELECT * FROM ethereum.core.ez_dex_swaps
    WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
    AND block_timestamp > '{start_date}'
    AND (token_in = '{WETH}' OR token_out = '{WETH}')
    AND (token_in = '{LOOKS}' OR token_out = '{LOOKS}')
    ORDER by block_timestamp ASC
"""

tx_sql_two_coins = f"""
    SELECT * FROM ethereum.core.ez_dex_swaps
    WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
    AND (token_in = '{WETH}' OR token_out = '{WETH}')
    AND (token_in = '{DC}' OR token_out = '{DC}')
    ORDER by block_timestamp ASC
"""

tx_sql_one_coin = f"""
    SELECT * FROM ethereum.core.ez_dex_swaps
    WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
    AND (token_in = '{DC}' OR token_out = '{DC}')
    ORDER by block_timestamp ASC
"""

tx_sql = f"""
    SELECT * FROM ethereum.core.ez_dex_swaps
    WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
    ORDER by block_timestamp ASC
"""


# print(tx_sql_one_coin)


# Default ShroomSDK function for making a query
def querying_pagination(query_string):
    """
        Standard API query code from ShroomDK, not original.
    """
    sdk = ShroomDK('cc8d77ef-eb03-4b7e-8914-f1db88324709')
    
    # Query results page by page and saves the results in a list
    # If nothing is returned then just stop the loop and start adding the data to the dataframe
    # print(query_string)
    result_list = []
    for i in range(1,11): # max is a million rows @ 100k per page
        data=sdk.query(query_string,page_size=100000,page_number=i)
        if data.run_stats.record_count == 0:  
            break
        else:
            result_list.append(data.records)
    
    result_df=pd.DataFrame()
    for idx, each_list in enumerate(result_list):
        if idx == 0:
            result_df=pd.json_normalize(each_list)
        else:
            result_df=pd.concat([result_df, pd.json_normalize(each_list)])
    
    return result_df



# Function for getting coin prices via a query from Flipside.xyz
def get_coin_prices(token_addresses):
    where2_str = f""

    for ind in range(len(token_addresses)):
      if ind < len(token_addresses) - 1:
        where2_str += f"'{token_addresses[ind]}',"
      else :
        where2_str += f"'{token_addresses[ind]}'"
      
    # print(where2_str)

    coin_price_sql = f"""
    SELECT t1.*
    FROM ethereum.core.fact_hourly_token_prices t1
    INNER JOIN
      (SELECT
        token_address
        , MAX(HOUR) AS MAXHOUR
      FROM ethereum.core.fact_hourly_token_prices
      GROUP BY token_address
      ) AS t2
    ON t1.token_address = t2.token_address
    AND t1.HOUR = t2.MAXHOUR
    WHERE t1.token_address in ({where2_str})
    """
    try: # error catching for undefined coins
        prices = querying_pagination(coin_price_sql)
        prices = [list(prices['symbol']), list(prices['token_address']), list(prices['price'])]
        result = {}
        for price in range(len(prices[0])):
          result[prices[1][price]] = [prices[0][price], prices[2][price]]
    except:
        result = -1
    
    return result

# print(get_coin_prices(['0xf3ae5d769e153ef72b4e3591ac004e89f48107a1', '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0'])) # DPR Coin, for optional testing of this function
# print(get_coin_prices(['0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0'])) # Matic Coin, for optional testing of this function


def get_coin_price(token_address, date):
    date = parser.parse(date) #datetime.strptime(date, '%y-%m-%d %H:%M:%S.%f')
    coin_price_sql = f"""
      SELECT *
      FROM ethereum.core.fact_hourly_token_prices
      WHERE token_address in ('{token_address}')
      AND hour > '{date - timedelta(hours=1)}'
      AND hour < '{date + timedelta(hours=1)}'
      LIMIT 1
      """
    # print("GETCOINPRICE CALLED")
    try: # error catching for undefined coins
        price = querying_pagination(coin_price_sql)
        price = float(price['price'])
        result = price
        # for price in range(len(prices[0])):
        #   result[prices[1][price]] = [prices[0][price], prices[2][price]]
    except:
        result = -1
    
    #print("GETCOINPRICE FINISHED")

    return result


# print(get_coin_price('0x5283d291dbcf85356a21ba090e6db59121208b44', '2023-02-16 04:52:35.000'))


def get_coins_prices(token_address):
    coin_price_sql = f"""
        SELECT *
        FROM ethereum.core.fact_hourly_token_prices
        WHERE token_address in ('{token_address}')
        ORDER BY hour DESC
        """

    # print(coin_price_sql)

    try: # error catching for undefined coins
        result = querying_pagination(coin_price_sql)
        # for price in range(len(prices[0])):
        #   result[prices[1][price]] = [prices[0][price], prices[2][price]]
    except:
        result = -1
    return result


WETH_prices = get_coins_prices("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")


def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))


def get_price(df_prices, date):
    price = 0
    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
    date = hour_rounder(date)
    df_prices["hour"] = pd.to_datetime(df_prices["hour"])
    lower_date = date - timedelta(hours=1)
    higher_date = date + timedelta(hours=1)
    df2 = df_prices.query(f"hour == '{date}'")
    price = df2['price'].values[0]
    # price = float(df_prices.loc[(df_prices['hour'] >= lower_date) & (df_prices['hour'] <= higher_date)])
    return float(price)


# str(datetime.now() - timedelta(days=1))


# get_price(WETH_prices, str(datetime.now()- timedelta(days=1)))


# WETH_prices


def sortFunc(tx):
  return tx[0]


# Function for parsing the swaps a user makes in the range provided via the sql query in the metas kernel
# Prints the owners accumulated balance over this time span, as well as a profit
# Profit is calculated as the difference between the net amount they invested(USD) minus the amount their holdings are worth(USD) over the time range provided
def parsing_swaps(addresses, df):
    user_txs = []
    user_coins = {}
    non_swaps = []
    problem_coins = []
    sells = 0
    """
        structure:
        coins : {
          ('coin_symbol', 'coin_address1') : {
            'rPnL' : [running realized PnL],
            'buys' : [['03-01-2023', 1, 1500], ['03-05-2023', 2, 2000], ...] # buy txs, in [date, tokens_bought, price_bought_at] format sorted with earliest buys first
          },
          ...
        }
        """
    coins = {
        
    }

    for index, row in df.iterrows():
      if row[4] in addresses: # it's row[4] because the 4th column is address for this specific table
          user_txs.append([row[1] # select block time 0
                          , row[2] # tx_hash 1
                          , row[4] # tx_from 2
                          , row[5] # tx_to 3
                          , row[7] # pool_name 4
                          , row[10] # amount_in_usd 5
                          , row[12] # amount_out_usd 6
                          , row[19] # symbol_in 7
                          , row[20] # symbol_out 8
                          , row[17] # token address in 9
                          , row[18] # token address out 10
                          , row[8] # event_name 11
                          , row[9] # tokens_in 12
                          , row[11] # tokens_out 13
                          ]) 
          
          # print(user_txs)
          block_time = user_txs[-1][0]
          block_date = datetime.strptime(block_time, "%Y-%m-%d %H:%M:%S.%f")
          WETH_price = get_price(WETH_prices, block_time)
          tx_hash = user_txs[-1][1]
          tx_from = user_txs[-1][2]
          tx_to = user_txs[-1][3]
          pool_name = user_txs[-1][4]
          #amount_in_usd = user_txs[-1][5]
          #if isinstance(amount_in_usd, str):
          #  amount_in_usd = float(user_txs[-1][5].replace(',',''))
            
          
          #amount_out_usd = amount_in_usd # coin_out_price * tokens_out # user_txs[-1][6]

          addy_in = user_txs[-1][9] # address of coin in - the one they sell
          addy_out = user_txs[-1][10] # address of coin out - the one they buy/swap to own more of
          coin_in = (user_txs[-1][7], addy_in)
          coin_out = (user_txs[-1][8], addy_out)
          event_name = user_txs[-1][11]
          tokens_in = user_txs[-1][12]
          if isinstance(tokens_in, str):
            tokens_in = float(user_txs[-1][12].replace(',',''))
          
          tokens_out = user_txs[-1][13]
          if isinstance(tokens_out, str):
            tokens_out = float(user_txs[-1][13].replace(',',''))

          if addy_in == WETH:
            amount_in_usd = tokens_in * WETH_price
            amount_out_usd = amount_in_usd
          elif addy_out == WETH:
            amount_out_usd = tokens_out * WETH_price
            amount_in_usd = amount_out_usd
        #   else: should account for the case where neither are WETH
             

          if math.isnan(amount_in_usd):
              amount_in_usd = 0
          if math.isnan(amount_out_usd):
              amount_out_usd = 0


          coin_out_price = amount_in_usd / tokens_out # get_coin_price(coin_out[1], block_time)
          coin_in_price = amount_in_usd / tokens_in # get_coin_price(coin_in[1], block_time) 

          
          
          tx = (amount_in_usd, amount_out_usd) # how much what you're giving the LP is worth, how much you get for giving that

          sell_tx = [block_time, tokens_in, coin_in_price, amount_in_usd] # how much what you're giving up paid out, removing tokens from portfolio
          buy_tx = [block_time, tokens_out, coin_out_price, amount_out_usd] # how much what you paid in for it cost, adding tokens to portfolio

          if event_name != 'Swap':
              non_swaps.append((tx_hash, tx, (addy_in,addy_out)))
          
          if coin_out not in coins:
            coins[coin_out] = {
              'rPnL' : 0,
              'rPnL-30' : 0,
              'rPnL-60' : 0,
              'rPnL-90' : 0,
              'buys' : [buy_tx],
              'sells' : [],
              'historical_buys': [buy_tx]
            }
          else:
            coins[coin_out]['buys'].append(buy_tx)
            coins[coin_out]['historical_buys'].append(buy_tx)

          if coin_in in coins and len(coins[coin_in]['buys']) > 0: # if it's a sell, we calculate realized pnl on the sell and add it to the running realized pnl
            # print("GOT TO A SELL")
            coins[coin_in]['sells'].append(sell_tx)
            sells += 1
            rpnl = coins[coin_in]['rPnL']
            rpnl_30 = coins[coin_in]['rPnL-30']
            rpnl_60 = coins[coin_in]['rPnL-60']
            rpnl_90 = coins[coin_in]['rPnL-90']
            buys = coins[coin_in]['buys']
            # print(buys)

            # print(f'SELLS: {sells}')
            # print(f'BUYS: {buys}')
            # for coin in coins:
            #   print(coin)
            #   print(coins[coin]['rPnL'])
            #   print(coins[coin]['buys'])
            #   print(coins[coin]['sells'])

            # for coin in coins:
            #   print(coin)
            #   print(coins[coin]['rPnL'])
            #   print(coins[coin]['buys'])
            #   print(coins[coin]['sells'])
            buys.sort(key=sortFunc)
            sell_price = sell_tx[2]
            tokens_to_sell = sell_tx[1]
            # print(f'Selling {tokens_to_sell} at a price of {sell_price}')

            while len(buys) > 0 and tokens_to_sell > 0:
              # print(buys)
              # print(f'tokens_to_sell: {tokens_to_sell}')
              first_in_buy = buys[0]
              buy_price = first_in_buy[2]
              tokens_resolved = first_in_buy[1]
              if tokens_resolved >= tokens_to_sell:
                rpnl += tokens_to_sell * (sell_price - buy_price)
                if block_date > datetime.today() - timedelta(days=90):
                  rpnl_90 += tokens_to_sell * (sell_price - buy_price)
                  if block_date > datetime.today() - timedelta(days=60):
                    rpnl_60 += tokens_to_sell * (sell_price - buy_price)
                    if block_date > datetime.today() - timedelta(days=30):
                      rpnl_30 += tokens_to_sell * (sell_price - buy_price)
                first_in_buy[1] -= tokens_to_sell
                # print(f"{tokens_to_sell} tokens resolved from a buy price of {buy_price}")
                # print("All tokens resolved.")
                tokens_to_sell = 0
              else:
                rpnl += tokens_resolved * (sell_price - buy_price)
                if block_date > datetime.today() - timedelta(days=90):
                  rpnl_90 += tokens_resolved * (sell_price - buy_price)
                  if block_date > datetime.today() - timedelta(days=60):
                    rpnl_60 += tokens_resolved * (sell_price - buy_price)
                    if block_date > datetime.today() - timedelta(days=30):
                      rpnl_30 += tokens_resolved * (sell_price - buy_price)
                buys.pop(0)
                # print(f"{tokens_resolved} tokens resolved from a buy price of {buy_price}")
                tokens_to_sell -= tokens_resolved
              
              # if len(buys) == 1:
                #!!! print(f'Running out of buys: {buys}, {buys[0][1]} tokens left that were bought, still have {tokens_to_sell} tokens left to sell')
            
            coins[coin_in]['rPnL'] = rpnl
            coins[coin_in]['rPnL-30'] = rpnl_30
            coins[coin_in]['rPnL-60'] = rpnl_60
            coins[coin_in]['rPnL-90'] = rpnl_90
            # print(f'CURRENT RPNL: {rpnl}')

    # print('\n\n')
    # res = [[coins[coin_info]] for coin_info in coins.keys()]
    # print(f'res: {res}')
    # print(f'coins: {coins}')
    res = []

    for coin in coins:
    #   print('\n')
      res.append([coin[0], coin[1], coins[coin]['rPnL'], coins[coin]['rPnL-90'], coins[coin]['rPnL-60'], coins[coin]['rPnL-30']])
    #   print(f"{coin}                    Realized PnL: {coins[coin]['rPnL']}")
    #   print(f"Remaining buys of this coin(assets not yet sold): {coins[coin]['buys']}")
    #   print(f"Sells of this coin: {coins[coin]['sells']}")
    #   print(f"Historical buys of this coin: {coins[coin]['historical_buys']}")
    # print(res)
    res_df = pd.DataFrame(res, columns=['Coin Symbol', 'Coin Address', 'Realized Lifetime PnL', 'Realized 90 day PnL', 'Realized 60 day PnL', 'Realized 30 day PnL',])
    return res_df


# df = querying_pagination(tx_sql)
df = querying_pagination(tx_sql)
#df2 = querying_pagination(tx_sql_one_coin)
# df = pd.read_csv('/content/looks.csv')


tx_sql


df.to_csv('out.csv')


# print(f'df: {df}')


res_df = parsing_swaps(addresses, df)


res_df

# print(res_df)


res_df[res_df['Coin Symbol']=='PEPE']


# parsing_swaps(addresses, df)


# columns = df.loc[0, :]


# columns


# df


# df.to_csv('output.csv')


# pd.set_option('display.max_columns', None)
# print(df.to_string)


# buys = []
# rpnl = 0 #realized pnl


# buys.append(['04-06-2023', 1, 1500])
# buys.append(['04-07-2023', 2, 2000])
# def sortFunc(tx):
#   return tx[0]
# buys.sort(key=sortFunc)
# # q1.put(('04-05-2023', 3, 1000))


# print(buys)


# sell_info = ('04-07-2023', 2, 2500)
# sell_price = sell_info[2]
# tokens_to_sell = sell_info[1]


# sell_info


# while tokens_to_sell > 0:
#   first_in_buy = buys[0]
#   buy_price = first_in_buy[2]
#   tokens_resolved = first_in_buy[1]
#   if tokens_resolved >= tokens_to_sell:
#     rpnl += tokens_to_sell * (sell_price - buy_price)
#     first_in_buy[1] -= tokens_to_sell
#     print(f"{tokens_to_sell} tokens resolved from a buy price of {buy_price}")
#     print("All tokens resolved.")
#     tokens_to_sell = 0
#   else:
#     rpnl += tokens_resolved * (sell_price - buy_price)
#     buys.pop(0)
#     print(f"{tokens_resolved} tokens resolved from a buy price of {buy_price}")
#     tokens_to_sell -= tokens_resolved


# rpnl





