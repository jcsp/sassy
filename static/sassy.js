
$(document).ready(function(){
  $.each(CHARTS, function(i, chart) {
    $('#charts').append(chart.name);
    $('#charts').append("<div id='" + chart.name + "'></div> ");
    console.log(chart);

    var chart_spec = {
      chart: {
        renderTo: chart.name,
        type: 'line',
        zoomType: 'xy'
      },
      xAxis: {
        type: 'datetime'
      },
      series: [{
        data: chart.data
      }]
    };
    console.log(chart_spec);
    new Highcharts.Chart(chart_spec);
  });
});