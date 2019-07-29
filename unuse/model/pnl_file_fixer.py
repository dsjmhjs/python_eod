# -*- coding: utf-8 -*-
import os
import pandas as pd
import MySQLdb


def get_10_126_db():
    conn = MySQLdb.connect(
        host="172.16.10.126",
        user="llh",
        passwd="llh@yansheng",
        db="jobs"
    )
    return conn


class PnlFileChange(object):

    def __init__(self, pnl_path, file_type, day, strategy):

        self.pnl_path = pnl_path
        self.file_type = file_type
        self.day = day
        self.strategy = strategy

    @staticmethod
    def day_format_change(day):
        if '-' in day:
            return day
        else:
            return '-'.join([day[:4], day[4:6], day[6:8]])

    @staticmethod
    def get_common_cols(df, df_template):
        cols_1 = df.columns
        cols_2 = df_template.columns
        return list(set(cols_1).intersection(set(cols_2)))

    def create_template(self):
        conn = get_10_126_db()
        sql = "select * from strategy_intraday_parameter"
        df = pd.read_sql(sql, conn)
        df['date'] = df['date'].astype('str')
        df = df[df['strategy_name'] == self.strategy]
        tickers = df['ticker'].unique()
        cols = [x for x in tickers]
        cols.insert(0, 'date')
        days = [x for x in df['date'].unique()]
        # if '2017-04-10' in days:
        #     days.remove('2017-04-10')

        # create empty template with tickers and days
        df_template = pd.DataFrame(columns=cols)
        df_template['date'] = days
        df_template.index = df_template['date']

        group = df.groupby('date')
        for day, data in group:
            # if day == '2017-04-10':
            #     continue
            day_tickers = [x for x in data['ticker'].unique()]
            df_template.loc[[day], day_tickers] = '0'

        df_template = df_template.drop('date', axis=1)
        del conn
        return df_template

    def pnl_fix_process(self):
        filename = '%s_%s_report_%s.csv' % (self.strategy, self.file_type, self.day_format_change(self.day))
        df = pd.read_csv(os.path.join(self.pnl_path, filename))
        df.index = df['date'].astype('str')
        cols = [str(x) for x in df.columns]
        df.columns = cols
        df_template = self.create_template()
        common_cols = self.get_common_cols(df, df_template)

        # data set in template, just keep date columns same, otherwise it will be wrong
        df_template[common_cols] = df[common_cols]
        df_template.loc['Cum_Ret'] = df.loc['Cum_Ret'].fillna(0)
        df_template.iloc[-1] = df_template.iloc[-1].fillna(0)
        df_template[self.strategy] = df[self.strategy]
        df_template.to_csv(os.path.join(self.pnl_path, 'temp.csv'), index=True)


if __name__ == '__main__':
    path = r'Z:\temp\luolinhua\demon\pnl_file'
    date = '20170407'
    file_type_ = 'ret'
    strat = 'StkIntraDayStrategy'
    PnlFileChange(path, file_type_, date, strat).pnl_fix_process()
