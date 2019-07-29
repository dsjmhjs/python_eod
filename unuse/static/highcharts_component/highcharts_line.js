/**
 * Created by luolinhua on 17-5-25.
 */
function create_highcharts_line(container_id, draw_data, valueDecimals, tag) {
    <!--series: [-->
            <!--{name: '1', data: [[1487808000000, 100], [1488153600000, 90]]},-->
            <!--{name: '2', data: [[1487808000000, 10], [1488153600000, 11]]},-->
            <!--{name: '3', data: [[1487808000000, 40], [1488153600000, 42]]},-->

    Highcharts.stockChart(container_id, {
        rangeSelector: {
            selected: 4
        },
        yAxis: {
            labels: {
            },
            plotLines: [{
                value: 0,
                width: 2,
                color: 'silver'
            }]
        },

        plotOptions: {
            column: {
                 pointPadding: 0.2,
                 borderWidth: 0
            }
        },
        tooltip: {
            pointFormat: '<span style="color:{series.color}">{series.name}</span>:' +
            ' <b>{point.y}' + tag + '</b><br/>',
            valueDecimals: valueDecimals,
            split: true
        },
        series: draw_data,

    });
}