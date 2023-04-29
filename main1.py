# import csv
# from shroomdk import ShroomDK
# from shroomdk.errors import QueryRunExecutionError, ServerError, QueryRunTimeoutError, QueryRunRateLimitError, UserError, SDKError
# import pandas as pd
# import math
# import requests
# from datetime import date
# from datetime import timedelta
# from Historic_Crypto import LiveCryptoData
# from Historic_Crypto import Cryptocurrencies
# # !pip install pandas_datareader
# import pandas_datareader as web

# # ShroomDK API key: cc8d77ef-eb03-4b7e-8914-f1db88324709
# # Alpha Vantage API key: KVWJYZAEZ4QF9NRI
# # Initialize `ShroomDK` with your API Key
# sdk = ShroomDK("cc8d77ef-eb03-4b7e-8914-f1db88324709")
# av = 'KVWJYZAEZ4QF9NRI'

# # Metas
# address = "0xe36a124caa7ee0b75a96a934499ce68dac6d9562"
# days = 40
# start_date = date.today() - timedelta(days=days)#'2023-02-01 12:00:00.000' # y-m-d 24hrt
# tx_sql = f"""
#     SELECT * 
#     FROM ethereum.core.ez_dex_swaps
#     WHERE ethereum.core.ez_dex_swaps.origin_from_address=LOWER('{address}')
#     AND block_timestamp > '{start_date}'
#     ORDER by block_timestamp DESC
# """

# def compile_coins(file, addresses):
#     with open(file, newline='') as csvfile:
#         spamreader = csv.reader(csvfile, quotechar='|')
#         user_txs = []
#         user_coins = {}
#         for row in spamreader:
#             # print(row)
#             for address in addresses:
#                 if row[4] == address: # it's row[4] because the 4th column is address for this specific table
#                     user_txs.append([row[1] # select block time 0
#                                     , row[2] # tx_hash 1
#                                     , row[4] # tx_from 2
#                                     , row[5] # tx_to 3
#                                     , row[7] # pool_name 4
#                                     , row[10] # amount_in_usd 5
#                                     , row[12] # amount_out_usd 6
#                                     , row[19] # symbol_in 7
#                                     , row[20]]) # symbol_out 8
#                     block_time = user_txs[-1][0]
#                     tx_hash = user_txs[-1][1]
#                     tx_from = user_txs[-1][2]
#                     tx_to = user_txs[-1][3]
#                     pool_name = user_txs[-1][4]
#                     amount_in_usd = user_txs[-1][5]
#                     amount_out_usd = user_txs[-1][6]
#                     coin_in = user_txs[-1][7]
#                     coin_out = user_txs[-1][8]
#                     tx = (amount_in_usd, amount_out_usd) # (amount_in_usd, amount_out_usd)
#                     if coin_in not in user_coins:
#                         user_coins[coin_in] = [].append(tx[1])    
#                     else:
#                         user_coins[coin_in] = user_coins[coin_in].append(tx[1])
#                     if coin_out not in user_coins:
#                         user_coins[coin_out] - [].append(tx[0])
#                     else:
#                         user_coins[coin_out] = user_coins[coin_out].append(tx[1])

# def querying_pagination(query_string):
#     """
#         Standard API query code from ShroomDK, not original.
#     """
#     sdk = ShroomDK('cc8d77ef-eb03-4b7e-8914-f1db88324709')
    
#     # Query results page by page and saves the results in a list
#     # If nothing is returned then just stop the loop and start adding the data to the dataframe
#     result_list = []
#     for i in range(1,11): # max is a million rows @ 100k per page
#         data=sdk.query(query_string,page_size=100000,page_number=i)
#         if data.run_stats.record_count == 0:  
#             break
#         else:
#             result_list.append(data.records)
    
#     result_df=pd.DataFrame()
#     for idx, each_list in enumerate(result_list):
#         if idx == 0:
#             result_df=pd.json_normalize(each_list)
#         else:
#             result_df=pd.concat([result_df, pd.json_normalize(each_list)])
    
#     return result_df



# def get_coin_price(token_address):
#     coin_price_sql = f"""
#         select *
#         from ethereum.core.fact_hourly_token_prices
#         where TOKEN_ADDRESS='{token_address}'
#         order by HOUR DESC
#         limit 1
#     """
#     try: # error catching for undefined coins
#         prices = querying_pagination(coin_price_sql)
#         price = prices['price'][0]
#     except:
#         price = -1
    
#     return price

# # print(get_coin_price('0xf3ae5d769e153ef72b4e3591ac004e89f48107a1')) # DPR

# def parsing_swaps(addresses, df):
#     user_txs = []
#     user_coins = {}
#     non_swaps = []
#     problem_coins = []
#     for index, row in df.iterrows() :
#         for address in addresses:
#             if row[4] == address: # it's row[4] because the 4th column is address for this specific table
#                 user_txs.append([row[1] # select block time 0
#                                 , row[2] # tx_hash 1
#                                 , row[4] # tx_from 2
#                                 , row[5] # tx_to 3
#                                 , row[7] # pool_name 4
#                                 , row[10] # amount_in_usd 5
#                                 , row[12] # amount_out_usd 6
#                                 , row[19] # symbol_in 7
#                                 , row[20] # symbol_out 8
#                                 , row[17] # token address in 9
#                                 , row[18] # token address out 10
#                                 , row[8] # event_name 11
#                                 , row[9] # tokens_in 12
#                                 , row[11] # tokens_out 13
#                                 ]) 
#                 block_time = user_txs[-1][0]
#                 tx_hash = user_txs[-1][1]
#                 tx_from = user_txs[-1][2]
#                 tx_to = user_txs[-1][3]
#                 pool_name = user_txs[-1][4]
#                 amount_in_usd = user_txs[-1][5]
#                 amount_out_usd = user_txs[-1][6]
#                 taddy_in = user_txs[-1][9] # address of coin in - the one they sell
#                 taddy_out = user_txs[-1][10] # address of coin out - the one they buy/swap to own more of
#                 coin_in = (user_txs[-1][7], taddy_in)
#                 coin_out = (user_txs[-1][8], taddy_out)
#                 event_name = user_txs[-1][11]
#                 tokens_in = user_txs[-1][12]
#                 tokens_out = user_txs[-1][13]

#                 if math.isnan(amount_in_usd):
#                     amount_in_usd = 0
#                 if math.isnan(amount_out_usd):
#                     amount_out_usd = 0
                
#                 tx = (amount_in_usd, amount_out_usd)

#                 in_info = (-1 * tx[0], tx_hash, taddy_in, taddy_out, -1 * tokens_in, tx)
#                 out_info = (tx[1], tx_hash, taddy_in, taddy_out, tokens_out, tx)

#                 if event_name != 'Swap':
#                     non_swaps.append((tx_hash, tx, (taddy_in,taddy_out)))
                
#                 if coin_in not in user_coins:
#                     user_coins[coin_in] = [in_info]
#                 else:
#                     user_coins[coin_in].append(in_info)
#                 if coin_out not in user_coins:
#                     user_coins[coin_out] = [out_info]
#                 else:
#                     user_coins[coin_out].append(out_info)
#     # print(f'Length of user_txs: {len(user_txs)}')
#     # print(user_txs)
#     for coin in user_coins:
#         net_invested_usd = sum([user_coins[coin][tx_ind][0] for tx_ind in range(len(user_coins[coin]))])
#         token_balance = sum([user_coins[coin][tx_ind][4] for tx_ind in range(len(user_coins[coin]))])
#         amount_withdrawn = -1 * sum([user_coins[coin][tx_ind][0] for tx_ind in range(len(user_coins[coin])) if user_coins[coin][tx_ind][0] < 0])
#         # coin_price = get_crypto_price(symbola=coin, symbolb='USD')
#         # coin_price = LiveCryptoData(f'{coin}-USD').return_data()['price'][0]
        
#         coin_price = get_coin_price(coin[1])
#         if coin_price >= 0:
#             # print(coin)
#             # print([coin_price, token_balance])
#             print(f'\n\n{coin} :\n')
#             # print(f'   {coin[0]} price: {coin_price}')
#             net_owned_usd = coin_price * token_balance
#             user_coins[coin].append(f'   Net owned in USD: {net_owned_usd}')
#             profit = net_owned_usd - net_invested_usd
#             user_coins[coin].append(f'   Profit for {coin}: {profit}')
#             print(f'   Net owned in USD: {net_owned_usd}')
#             print(f'   Profit for {coin}: {profit}')
#         else:
#             problem_coins.append(coin)
#             print(f'\n\nCoin {coin} does not have a price upon query from Flipside.xyz')

#         user_coins[coin].append(f'   Current token balance: {token_balance} tokens')
#         user_coins[coin].append(f'   Amount withdrawn so far: ${amount_withdrawn}')
#         user_coins[coin].append(f'   Net USD invested: ${net_invested_usd}')
        
#         for coin_tx in user_coins[coin]:
#             print(f'   {coin_tx} \n')
        
#     print(f'\n\nnon_swaps: {non_swaps}')
#     print(f'problem coins(price undef, etc.): {problem_coins}')

# df = querying_pagination(tx_sql)
# columns = df.loc[0, :]
# # print(f'Columns:\n{columns.to_string()} \n\n Df:\n{df}')

# parsing_swaps([address], df)