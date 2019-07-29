# -*- coding: utf-8 -*-
import allure
from eod_aps.task.eod_check_task import *
from eod_aps.task.server_manage_task import *

date_utils = DateUtils()


@allure.feature('Check Jobs Run Test')
class TestCheckJobs(object):
    @allure.story('Run Order Check')
    def test_order_check(self):
        order_check()
        assert True

    @allure.story('Run DB Check')
    def test_db_check(self):
        db_check_am()
        assert True

    @allure.story('Run After Start Check')
    def test_after_start_check_am(self):
        after_start_check_am()
        assert True

    @allure.story('Run Position Check')
    def test_pf_real_position_check_am(self):
        pf_real_position_check_am()
        assert True

    @allure.story('Run Server Status Check')
    def test_server_status_check(self):
        server_status_check()
        assert True

    # @allure.story('Run Account Position Report Check')
    # def test_account_position_report(self):
    #     account_position_report()
    #     assert True
    #
    # @allure.story('Run MainContract Change Check')
    # def test_main_contract_change_check(self):
    #     main_contract_change_check()
    #     assert True
    #
    # @allure.story('Run DbCompare Check')
    # def test_db_compare(self):
    #     db_compare()
    #     assert True
    #
    # @allure.story('Run check_after_market_close')
    # def test_check_after_market_close(self):
    #     check_after_market_close()
    #     assert True

    # @allure.story('Run server_order_monitor')
    # def test_server_order_monitor(self):
    #     server_order_monitor()
    #     assert True

    # @allure.story('Run server_risk_validate')
    # def test_server_risk_validate(self):
    #     server_risk_validate()
    #     assert True



