from flask import Flask, request, render_template, session, redirect
from datetime import timedelta
from datetime import datetime
from datetime import date
# from p02 import res_df
from p02 import querying_pagination
from p02 import parsing_swaps
import p02

app = Flask(__name__) 


# print(res_df)
addresses = ["0xb5e8add227067bf3db9989686285919fb669b580"] # BLUR/LOOKS"0x3471884f189fd7c63fe8c83601d28ce0cc1b3853"]# (BLUR guy, 0x3471884f189fd7c63fe8c83601d28ce0cc1b3853), 0x86b575f63243a434c9a6d61b567d1a20e6c4bab3, 0xf3bed2bde0510ff5a058bc82cf0dcda28cc0dc42, 0x09029f5af07388127995ba31060fb314c5d79972
where1_str = f""
for ind in range(len(addresses)):
  if ind < len(addresses) - 1:
    where1_str += f"'{addresses[ind]}',"
  else :
    where1_str += f"'{addresses[ind]}'"
tx_sql = f"""
    SELECT * FROM ethereum.core.ez_dex_swaps
    WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
    ORDER by block_timestamp ASC
"""
df = querying_pagination(tx_sql)
print(f'p02.df: {p02.df}')
res_df = parsing_swaps(addresses, df)
print(f'res_df: {res_df}')

@app.route('/', methods=("POST", "GET"))
def home():
  # print('hi')
  if 'addresses' in request.args:
    # print([request.args['addresses']])
    global addresses, df, res_df, where1_str, tx_sql
    addresses = request.args['addresses'].split(',')
    where1_str = f""
    for ind in range(len(addresses)):
      if ind < len(addresses) - 1:
        where1_str += f"'{addresses[ind]}',"
      else :
        where1_str += f"'{addresses[ind]}'"
    tx_sql = f"""
        SELECT * FROM ethereum.core.ez_dex_swaps
        WHERE ethereum.core.ez_dex_swaps.origin_from_address in ({where1_str})
        ORDER by block_timestamp ASC
    """
    df = querying_pagination(tx_sql)
    res_df = parsing_swaps(addresses, df)
    # print(f'res_df: {res_df}')
  return render_template('simple.html',  tables=[res_df.to_html(classes='data', header="true")])




if __name__ == "__main__":
  app.run(debug=True) 