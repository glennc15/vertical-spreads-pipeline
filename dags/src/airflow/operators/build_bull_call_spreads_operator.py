import pandas as pd
import numpy as np
import pytz

import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from .spreads_etl_base import SpreadsEtlBase


class BuildBullCallSpreadsOperator(SpreadsEtlBase):

    def __init__(
        self,
        postgres_conn_id,
        mongo_conn_id,
        sql_path,
        **kwargs
    ):

        super().__init__(
            postgres_conn_id=postgres_conn_id,
            mongo_conn_id=mongo_conn_id,
            sql_path=sql_path,
            **kwargs
        )


    def execute(self, context):

        # initialize datebase connections:
        self._pg_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id
        )

        self._mongo_hook = MongoHook(
            mongo_conn_id=self._mongo_conn_id
        )

        # get the most currect spot record from postgres:
        with open(self._sql_path, "r") as f:

            postgres_records = self.fetch_postgres_records(
                query=f.read(),
                expected_records=False,
                allow_zero=True
            )

        # build spreads for each expiration:

        expirations_generator = self.get_mongo_expirations(
            db="OptionData",
            collection="SPY_Options",
            timestamp=postgres_records[0][1]
        )

        for expiraiton in expirations_generator:
            call_options = self.get_call_options(
                db="OptionData",
                collection="SPY_Options",
                query={
                    "timestamp": postgres_records[0][1],
                    "expiration": expiraiton.get("expiration")
                },
            )

            # build the SQL query to get the ohlc record from Postgres.
            # the expirations in mongo are US/Central timezone. But they are stored in mongo
            # as UTC. So converting them back to US/Central:
            utc_tz = pytz.timezone('UTC')
            central_tz = pytz.timezone('US/Central')
            this_expiration = utc_tz.localize(expiraiton.get('expiration')).astimezone(central_tz)

            query = f"SELECT * FROM spy_ohlc WHERE ohlc_date = '{this_expiration.date()}';"

            ohlc_record = self.fetch_postgres_records(
                query=query,
                expected_records=1,
                allow_zero=True
            )

            try:
                expiration_spot = float(ohlc_record[0][4])

            except Exception as e:
                expiration_spot = None
                print(f"No expiration spot for expirations = {this_expiration}")
                print(f"{ohlc_record}")


            bull_call_spreads = self.build_bull_call_spreads(
                call_options=call_options,
                spot=postgres_records[0][2],
                expiration_spot=expiration_spot,
                expiration=expiraiton.get('expiration'),
                spot_timestamp=call_options[0].get('timestamp')

            )

            # write the data frame to Postgres:
            bull_call_spreads.to_sql(
                name="bull_calls",
                con=self._pg_hook.get_sqlalchemy_engine(),
                index=False,
                if_exists='append',
            )


    def build_bull_call_spreads(self, call_options, spot, expiration_spot, expiration, spot_timestamp):
        '''

        call_options: list of dicts
        spot: float
        expiration: datetime

        '''

        def bull_call_profit(df_row):
            if df_row['expiration_close'] >= df_row['short_strike']:
                profit = df_row['strike_delta'] / -df_row['risk']

            elif df_row['expiration_close'] <= df_row['long_strike']:
                profit = df_row['risk']

            elif df_row['long_strike'] < df_row['expiration_close'] < df_row['short_strike']:
                profit = abs(df_row['expiration_close']-df_row['break_even']) / -df_row['risk']

            else:
                raise ValueError(f"error with {df_row}")


            return profit

        # calls dataframe:
        calls_df = pd.DataFrame(call_options)
        calls_df = calls_df.sort_values(['strike'])
        calls_df = calls_df.reset_index()

        # bull call spreads:
        descriptions = calls_df['description'].to_list()
        long_descriptions = list()
        short_descriptions = list()
        for idx in range(0, (len(descriptions)-1)):
            shorts = descriptions[(idx+1):]
            long_descriptions += np.repeat(descriptions[idx], len(shorts)).tolist()
            short_descriptions += shorts

            assert len(long_descriptions) == len(short_descriptions)

        calls_df = calls_df.set_index("description")

        bull_calls_df = pd.DataFrame(
            {
                "id": [self.get_uuid7() for x in range(len(long_descriptions))],
                "long_description": long_descriptions,
                "short_description": short_descriptions
            }
        )

        bull_calls_df['expiration'] = expiration
        bull_calls_df['spot_timestamp'] = spot_timestamp
        bull_calls_df['spot'] = spot

        bull_calls_df['short_strike'] = calls_df.loc[short_descriptions]['strike'].values
        bull_calls_df['long_strike'] = calls_df.loc[long_descriptions]['strike'].values
        bull_calls_df['strike_delta'] = bull_calls_df['short_strike'] - bull_calls_df['long_strike']

        bull_calls_df['risk'] = calls_df.loc[short_descriptions]['bid'].values - calls_df.loc[long_descriptions]['ask'].values
        bull_calls_df['max_profit'] = bull_calls_df['strike_delta'] + bull_calls_df['risk']
        bull_calls_df['break_even'] = bull_calls_df['long_strike'] - bull_calls_df['risk']

        bull_calls_df['delta'] = calls_df.loc[long_descriptions]['delta'].values - calls_df.loc[short_descriptions]['delta'].values

        bull_calls_df['long_iv'] = calls_df.loc[long_descriptions]['iv'].values
        bull_calls_df['short_iv'] = calls_df.loc[short_descriptions]['iv'].values
        bull_calls_df['expiration_close'] = expiration_spot

        if expiration_spot:
            bull_calls_df['profit'] = bull_calls_df.apply(bull_call_profit, axis=1)

        else:
            bull_calls_df['profit'] = None


        bull_calls_df['time_to_expiration'] = (expiration - spot_timestamp).total_seconds()
        bull_calls_df['past_expiration'] = datetime.datetime.now() > expiration

        bull_calls_df = bull_calls_df.loc[bull_calls_df['max_profit'].ge(0)]

        bull_calls_df = bull_calls_df.reset_index()

        return bull_calls_df

