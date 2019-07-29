# coding: utf-8
import calendar
import datetime
import os
import pandas as pd
import numpy as np
from eod_aps.model.schema_jobs import HardWareInfo
from eod_aps.model.schema_portfolio import RealAccount, PfAccount
from eod_aps.model.schema_history import PhoneTradeInfo
from eod_aps.model.schema_strategy import StrategyGrouping
from eod_aps.tools.tradeplat_position_tools import RiskView, InstrumentView
from flask import render_template, request, current_app, flash, redirect, url_for, jsonify, make_response
import json
from flask_login import login_required
from eod_aps.model.schema_history import ServerRisk
from eod_aps.model.server_constans import server_constant
from eod_aps.model.eod_const import const, CustomEnumUtils
from eod_aps.tools.phone_trade_tools import send_phone_trade
from eod_aps.tools.date_utils import DateUtils
from eod_aps.tools.common_utils import CommonUtils
from flask import Blueprint

statistic_module = Blueprint('statistic_module', __name__)

from . import view
#
#
# if __name__ == '__main__':
#     print(const.EOD_CONFIG_DICT['stock_basic_data_dict']['300163'])
