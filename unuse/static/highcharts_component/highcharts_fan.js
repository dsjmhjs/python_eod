/**
 * Created by luolinhua on 17-5-26.
 */
function create_highcharts_fan(container_id, series_data, basic_config) {
    $('#'+container_id).highcharts({
       chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false
       },
       title: {
           text: basic_config['title']
       },
       tooltip: {
           headerFormat: '{series.name}<br>',
           pointFormat: '{point.name}: <b>{point.percentage:.1f}%</b>'
       },
       plotOptions: {
           pie: {
               allowPointSelect: true,
               cursor: 'pointer',
               dataLabels: {
                   enabled: true,
                   format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                   style: {
                       color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'white'
                   }
               }
           }
       },
       series: [{
           type: 'pie',
           name: basic_config['subtitle'],
           data: [
               ['Firefox',   45.0],
               ['IE',       26.8],
               {
                   name: 'Chrome',
                   y: 12.8,
                   sliced: true,
                   selected: true
               },
               ['Safari',    8.5],
               ['Opera',     6.2],
               ['hello',   0.7]
           ]
       }]
    });
}
