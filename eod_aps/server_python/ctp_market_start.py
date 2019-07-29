# -*- coding: utf-8 -*-
import os
from eod_aps.server_python import *

if __name__ == '__main__':
    os.chdir(DATAFETCHER_PROJECT_FOLDER)
    os.popen('./script/stop.fetch_market.sh')
    os.popen('./script/start.fetch_market.sh')


