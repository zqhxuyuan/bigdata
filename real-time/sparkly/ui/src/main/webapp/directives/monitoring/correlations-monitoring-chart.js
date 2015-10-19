app.directive('correlationsMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/correlations-monitoring-chart.html',
		scope : {
			clusterid : "=",
			componentid: "=",
            componentname : "=",
			monitoring: "="
		},
		link : function(scope, element, attrs) {
		    scope.timeSeries = {};
		    scope.width = Math.floor($(element).width() / 2);
		    scope.size = Math.min(scope.width, 300);

		    scope.time_series_options = {
                type: 'line',
                series_names: ['correlation'],
                value_names: [],
                y_range: [-1.0, 1.0],
                legend: false
            };

            scope.dataSource = new MonitoringDataSource(scope.clusterid, scope.componentid, scope.monitoring.name);
            scope.dataSource.listen(function(event){
                scope.addEvent(event);
            }, function(pastData) {
                scope.timeSeries.init(scope.time_series_options, []);
                $(pastData).each(function(index, event){scope.addEvent(event);});
            });


            scope.features = [];
            scope.correlations = {};
            scope.selectedCorrelation = null;

            scope.addEvent = function(event) {
                var time = Math.round(event.timestamp / 1000);
                scope.updateFeatures(event.data);
                scope.update_correlations_plot(event);
            };

            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });

            scope.updateFeatures =function (data) {
                scope.features = [];
                for (var property in data) {
                    if (data.hasOwnProperty(property)) {
                        var parts = property.split(".");
                        if(!scope.features.contains(parts[0])) {
                            scope.features.push(parts[0]);
                        }
                        if(!scope.features.contains(parts[1])) {
                            scope.features.push(parts[1]);
                        }
                    }
                }
            };

            scope.update_correlations_plot = function (event) {
                var padding = 4;
                var color = d3.scale.linear().domain([-1, 1]).range(["#e74c3c", "#2ecc71"]);
                var formatValue = d3.format(",.2f");
                var cell_size = (scope.size / scope.features.length) - padding;

                for (var combination in event.data) {
                    if (event.data.hasOwnProperty(combination)) {
                        var value = event.data[combination];
                        var parts = combination.split(".");
                        var x = scope.features.indexOfObject(parts[0]) * (cell_size + padding);
                        var y = scope.features.indexOfObject(parts[1]) * (cell_size + padding);

                        var correlation = scope.correlations[combination];
                        if(correlation == null) {
                            correlation = {
                                "name" : parts[0] + "/" + parts[1],
                                "history": FixedQueue(100)
                            };
                            scope.correlations[combination] = correlation;
                        }

                        correlation.value = value;
                        correlation.opacity = Math.abs(value);
                        correlation.color = color(value);
                        correlation.tooltip = correlation.name + " : " + formatValue(value);
                        correlation.position = "translate(" + x + "," + y + ")";
                        correlation.size = cell_size;
                        var correlation_event = {'timestamp': event.timestamp, data: {'correlation': value}};
                        correlation.history.push(correlation_event);
                        if(scope.selectedCombination == combination) {
                            scope.timeSeries.add(correlation_event);
                        }
                    }
                }

                // Remove deprecated correlations
                for (var combination in scope.correlations) {
                    if (scope.correlations.hasOwnProperty(combination) && !event.data.hasOwnProperty(combination)) {
                        delete scope.correlations[combination]
                    }
                }

                // Init time series data
                if(typeof scope.selectedCombination === 'undefined' && Object.keys(scope.correlations).length > 0) {
                    scope.show_correlation_history(Object.keys(scope.correlations)[0]);
                }
            };

            scope.show_correlation_history = function(combination) {
                scope.selectedCombination = combination;

                scope.timeSeries.title(scope.componentname + " : " + scope.correlations[combination].name);
                scope.timeSeries.replaceData(scope.correlations[combination].history);
            };
        }
	};
});