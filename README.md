# swing_trading_bot

Here is my first created trading bot for a cryptocurrency. It was deployed on GCP with a cloud scheduler and written in Python.
Imo there is a huge difference compared to the data analytics. I would say the bot is more like a data engineering solution. 

The bot consists of the following steps:

- Import neccessary libraries
- Connection to the DB, query the data and delete the "old" data
- Single point data fetch from the KUCOIN-API
- Concat the old Master-DF with the new single data point
- Calculate features and assign signals (buy/sell or nothing)
- Control-Flow based on the latest signal --> Buy/Sell Orders via KUCOIN-API 
- Insertion of the new Master-DF to the DB
- End

Since this repo is available for the public, one major component is deleted in the uploaded version. Because for safety (unintended use of third parties).

###########################################################################################################

DON'T USE THIS BOT UNLESS YOU REALLY KNOW WHAT YOU ARE DOING!
I WILL NOT BE RESPONSIBLE FOR UNINTENDED USE OF THIRD PARTIES!

###########################################################################################################
