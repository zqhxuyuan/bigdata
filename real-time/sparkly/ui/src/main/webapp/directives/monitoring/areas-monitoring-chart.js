app.directive('areasMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/areas-monitoring-chart.html',
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
                    type: 'area', unstack: true,
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