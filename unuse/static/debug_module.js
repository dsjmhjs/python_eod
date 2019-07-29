$(document).ready(function(){
    $(".chosen_type").click(function(){
        var get_id = $(this).attr("id");
        var group = get_id.split("#");
        var target_id = group[0];
        $('#'+target_id).text(target_id + ": " + group[1]);
    })

    $("#send_config").click(function(){
        // alert($("#show_limit_num").val());
        var config_ = {};
        var vars = ["stock_num", "data_freq", "WindA", "sort_rules"];
        var text = "lack config variable: ";
        var if_all_ok = true;

        for(var i = 0; i < vars.length; i++){
            var group = ($("#"+vars[i]).html()).split(": ");
            if(group.length > 1){
                config_[vars[i]] = group[1];

            }
            else{
                if_all_ok = false;
                text += vars[i] + ",";
            }
        }

        if(if_all_ok){
            // send config to back platform

            var for_wind = true;
            // for_wind = config_["WindA"]
            if (config_["WindA"] == "not WindA"){
                for_wind = false;
            }
            var show_limit_num = '8';
            if($('#show_limit_num').val() != ""){
                show_limit_num = $('#show_limit_num').val()
            }
            var temp_data = {
                config: JSON.stringify({
                    "freq": config_['data_freq'],
                    "for_wind": for_wind,
                    "stock_num": config_["stock_num"],
                    "sort_rules": config_["sort_rules"],
                    "show_limit_num": show_limit_num,
                })
            };

            $.ajax({
                url: "/user/luolinhua",
                type: 'POST',
                data: temp_data,
                data_type: 'json',
                success: function(msg){
                   var get_data = JSON.parse(msg);
                   var chart = {
                      type: 'column'
                   };
                   var title = {
                      text: get_data['title']
                   };
                   var subtitle = {
                      text: 'data_frequency: ' + get_data['data_frequency']
                   };
                   var xAxis = {
                      categories: ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
                      crosshair: true
                   };

                   var new_xAxis = {
                      categories: get_data['strategy'],
                      crosshair: true
                   };

                   var yAxis = {
                      min: get_data['min'],
                      title: {
                         text: '收益情况'
                      }
                   };
                   var tooltip = {
                      headerFormat: '<span style="font-size:10px">{point.key}</span><table>',
                      pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
                         '<td style="padding:0"><b>{point.y:.6f}</b></td></tr>',
                      footerFormat: '</table>',
                      shared: true,
                      useHTML: true
                   };
                   var plotOptions = {
                      column: {
                         pointPadding: 0.2,
                         borderWidth: 0
                      }
                   };
                   var credits = {
                      enabled: false
                   };

                   var series= [{
                        name: 'Tokyo',
                            data: [49.9, 71.5, 106.4, 129.2, 144.0, 176.0, 135.6, 148.5, 216.4, 194.1, 95.6, 54.4]
                        }, {
                            name: 'New York',
                            data: [83.6, 78.8, 98.5, 93.4, 106.0, 84.5, 105.0, 104.3, 91.2, 83.5, 106.6, 92.3]
                        }, {
                            name: 'London',
                            data: [48.9, 38.8, 39.3, 41.4, 47.0, 48.3, 59.0, 59.6, 52.4, 65.2, 59.3, 51.2]
                        }, {
                            name: 'Berlin',
                            data: [42.4, 33.2, 34.5, 39.7, 52.6, 75.5, 57.4, 60.4, 47.6, 39.1, 46.8, 51.1]
                   }];
                   var new_series = new Array();
                   var report_data = get_data['report_data'];
                   var period = get_data['period']
                   for(var i = 0; i < get_data['length'];i++){
                        var dict_ = {
                            name: period[i],
                            data: report_data[period[i]]
                        };
                        new_series[i] = dict_
                   }

                   var json = {};
                   json.chart = chart;
                   json.title = title;
                   json.subtitle = subtitle;
                   json.tooltip = tooltip;
                   json.xAxis = new_xAxis;
                   json.yAxis = yAxis;
                   json.series = new_series;
                   json.plotOptions = plotOptions;
                   json.credits = credits;
                   $('#container').highcharts(json);
                }   // end success
            });   // end ajax
        }
        else{
            text += " please input your variable."
            return;
        }
    })

});