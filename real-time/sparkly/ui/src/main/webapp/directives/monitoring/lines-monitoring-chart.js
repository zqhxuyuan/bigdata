app.directive('linesMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/lines-monitoring-chart.html',
		scope : {
			clusterid : "=",
			componentid: "=",
            componentname : "=",
			monitoring: "="
		},
		link : function(scope, element, attrs) {
		    scope.timeSeriesControl = {};

            scope.dataSource = new MonitoringDataSource(scope.clusterid, scope.componentid, scope.monitoring.name);
            scope.dataSource.listen(function(event){
                scope.timeSeriesControl.add(event, true);
            }, function(pastData) {
                var options = {
                    title: scope.componentname,
                    type: 'line',
                    series_names: scope.monitoring.values,
                    value_names: scope.monitoring.primaryValues,
                    unit: scope.monitoring.unit
                };
                scope.timeSeriesControl.init(options, pastData);
            });

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });
        }
	};
});