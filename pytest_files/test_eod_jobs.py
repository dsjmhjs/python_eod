# -*- coding: utf-8 -*-
# content of test_class.py
import allure
from eod_aps.job.aggregator_manager_job import *
from eod_aps.task.server_manage_task import *


date_utils = DateUtils()


@allure.feature('Aggregator Test')
class TestEodJobs(object):
    @allure.story('Run Aggregator')
    def test_run_aggregator(self):
        with allure.step("Start Aggregator Restart step."):
            restart_aggregator_day()
            # allure.attach('Run times', '1s')
        with allure.step("Start Aggregator Validate step."):
            aggregator_model = server_constant.get_server_model('aggregator')
            run_result = aggregator_model.run_cmd_str('screen -ls')
            assert 'Aggregator' in run_result, 'Aggregator Start Error!'

        time.sleep(3)
        with allure.step("Stop Aggregator."):
            stop_aggregator()
        with allure.step("Stop Aggregator Validate step."):
            aggregator_model = server_constant.get_server_model('aggregator')
            run_result = aggregator_model.run_cmd_str('screen -ls')
            assert 'Aggregator' not in run_result, 'Aggregator Kill Error!'

        with allure.step("Start Aggregator Night step."):
            start_aggregator_night()
        with allure.step("Start Aggregator Validate step."):
            aggregator_model = server_constant.get_server_model('aggregator')
            run_result = aggregator_model.run_cmd_str('screen -ls')
            assert 'Aggregator' in run_result

        time.sleep(3)
        with allure.step("Stop Aggregator."):
            stop_aggregator()
        with allure.step("Stop Aggregator Validate step."):
            aggregator_model = server_constant.get_server_model('aggregator')
            run_result = aggregator_model.run_cmd_str('screen -ls')
            assert 'Aggregator' not in run_result

    @allure.story('Update Server DB')
    def test_update_server_db(self):
        # TradingTime Check
        validate_time = date_utils.get_today_str('%H%M%S')
        if not (90000 < int(validate_time) < 153000):
            assert True
        else:
            server_list = server_constant.get_trade_servers()
            with allure.step("Start Update Position."):
                update_position_job(server_list)

            # with allure.step("Start Update Price."):
            #     start_update_price()
            assert True

    @allure.story('Run TradePlat')
    def test_run_tradeplat(self):
        server_list = server_constant.get_trade_servers()
        with allure.step("Start TradePlat step."):
            start_flag = start_servers_tradeplat(server_list)
            assert start_flag

        with allure.step("Stop TradePlat step."):
            stop_servers_tradeplat(server_list)

        with allure.step("Stop TradePlat Validate step."):
            for server_name in server_list:
                server_model = server_constant.get_server_model(server_name)
                run_result = server_model.run_cmd_str('screen -ls')
                assert 'MainFrame' not in run_result
