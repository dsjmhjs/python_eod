/**
 * Created by luolinhua on 17-5-25.
 */
function create_highcharts_bar(container_id, series, basic_config, valueDecimals, tag){
    // basic_config: title, subtitle, xAxis, yAxis
   var bar_config = {
       chart:{
           type: 'column'
       },
       title:{
           text: basic_config['title']
       },
       subtitle:{
           text: basic_config['subtitle']
       },
       xAxis:{
           categories: basic_config['xAxis'],
           labels:{
               rotation: -30,
               overflow: undefined
           }
       },
       yAxis:{
           title: {
               text: basic_config['yAxis']
           }
       },
       tooltip:{
           headerFormat: '<span style="font-size:10px">{point.key}</span><table>',
           pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
           '<td style="padding:0; color:white"><b>{point.y}' + tag + '</b></td></tr>',
           footerFormat: '</table>',
           valueDecimals: valueDecimals,
           shared: true,
           useHTML: true
       },
       plotOptions:{
           column: {
             pointPadding: 0.2,
             borderWidth: 0
           }
       },
       series: series // [{name:'', data:[]}, {}, {}]
   };

   $('#' + container_id).highcharts(bar_config);
}