app.directive('scatterPlotMonitoringChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/scatter-plot-monitoring-chart.html',
		scope : {
			clusterid : "=",
			componentid: "=",
            componentname : "=",
			monitoring: "="
		},
		link : function(scope, element, attrs) {
            scope.dataSource = new MonitoringDataSource(scope.clusterid, scope.componentid, scope.monitoring.name);
            scope.dataSource.listen(function(event){
                scope.addEvent(event);
            });


            scope.features = [];
            scope.plot_size = Math.min(300, Math.floor($(element).width()));
            scope.cell_padding = 2;

            scope.addEvent = function(event) {
                var data = event.data;
                if(data == null) {
                    return;
                }

                scope.features = data.histograms.map(function(hist) {return hist.feature;});
                scope.n = scope.features.length;
                scope.bins = data.histograms[0].bins.length;
                scope.cell_size = (scope.plot_size / scope.n) - scope.cell_padding;
                scope.bin_size = scope.cell_size / scope.bins;

                // Domains
                scope.feature_domains = {};
                for(var i = 0; i < scope.n; i++) {
                    var feature = scope.features[i];
                    var bins = data.histograms[i].bins;
                    scope.feature_domains[feature] = [bins[0].x1, bins[bins.length - 1].x2];
                }

                // Data
                scope.histograms = scope.compute_histograms_1D(data.histograms);
                scope.samples_2D = scope.compute_samples_2D(data.samples);
                scope.histograms_2D = scope.compute_histograms_2D(data.samples);
            };

            scope.cell_position = function(i, j) {
                return "translate(" + i * (scope.cell_size + scope.cell_padding) + ", " + j * (scope.cell_size + scope.cell_padding) + ")";
            };

            scope.compute_histograms_1D = function(histograms) {
                return histograms.map(function(hist, hist_index){
                    var height_scale = d3.scale.linear().domain([0, d3.max(hist.bins.map(function(bin) {return bin.count}))]).range([scope.cell_size - 12, 0]);
                    return {
                        transform: scope.cell_position(hist_index, hist_index),
                        feature: hist.feature,
                        bins: hist.bins.map(function(bin, i){
                            return {
                                x: i * scope.bin_size,
                                y: 12 + (isNaN(bin.count) ? 0 : height_scale(bin.count)),
                                height: (scope.cell_size - 12 - (isNaN(bin.count) ? 0 : height_scale(bin.count))),
                                tooltip: hist.feature + " [" + bin.x1.toFixed(2) + "," + bin.x2.toFixed(2) + "] : " + Math.floor(bin.count)
                            };
                        })
                    };
                });
            }

            scope.compute_samples_2D = function(all_samples) {
                var samples_2D = [];
                for(var i = 0; i < scope.n; i++) {
                    for(var j =  i +1; j < scope.n; j++) {
                        var feature1 = scope.features[i];
                        var feature2 = scope.features[j];
                        var x_scale = d3.scale.linear().range([0, scope.cell_size]).domain(scope.feature_domains[feature1]);
                        var y_scale = d3.scale.linear().range([scope.cell_size, 0]).domain(scope.feature_domains[feature2]);

                        samples_2D.push({
                            transform: scope.cell_position(scope.features.indexOfObject(feature1), scope.features.indexOfObject(feature2)),
                            feature1: feature1,
                            feature2: feature2,
                            samples: all_samples.map(function(features) { return {x: x_scale(features[i]), y: y_scale(features[j])};})
                        });
                    }
                }
                return samples_2D;
            };

            scope.compute_histograms_2D = function(all_samples) {
                var histograms_2D = [];
                for(var k = 0; k < scope.n; k++) {
                    for(var l = k + 1; l < scope.n; l++) {
                        var domain_x = scope.feature_domains[scope.features[l]];
                        var domain_y = scope.feature_domains[scope.features[k]];
                        var step_x = (domain_x[1] - domain_x[0]) / scope.bins;
                        var step_y = (domain_y[1] - domain_y[0]) / scope.bins;

                        var bins = [];
                        for(var i = 0; i < scope.bins; i++) {
                            for(var j = 0; j < scope.bins; j++) {
                                var x = i * scope.bin_size;
                                var y = j * scope.bin_size;

                                var x1 = domain_x[0] + i * step_x;
                                var x2 = x1 + step_x;
                                var y1 = domain_y[0] + j * step_y;
                                var y2 = y1 + step_y;

                                var index = i * scope.bins + j;
                                bins[index] = {
                                    x: x,
                                    y: y,
                                    count: 0,
                                    tooltip: scope.features[l] + " [" + x1.toFixed(2) + "," + x2.toFixed(2) + "], " + scope.features[k] + " [" + y1.toFixed(2) + "," + y2.toFixed(2) + "] : "
                                };
                            }
                        }

                        all_samples.forEach(function(features) {
                            var i = Math.min(Math.floor((features[l] - domain_x[0]) / step_x), scope.bins - 1);
                            var j = Math.min(Math.floor((features[k] - domain_y[0]) / step_y), scope.bins - 1);
                            var index = i * scope.bins + j
                            bins[index].count = bins[index].count + 1;
                        });

                        var opacity_scale = d3.scale.linear().domain([0, d3.max(bins.map(function(bin) {return bin.count}))]).range([0.0, 1.0]);
                        for(var i = 0; i < bins.length; i++) {
                            bins[i].opacity = opacity_scale(bins[i].count);
                            bins[i].tooltip = bins[i].tooltip + Math.floor(bins[i].count);
                        }

                        histograms_2D.push({
                            transform: scope.cell_position(scope.features.indexOfObject(scope.features[l]), scope.features.indexOfObject(scope.features[k])),
                            feature1: scope.features[l],
                            feature2: scope.features[k],
                            bins: bins
                        });
                    }
                }
                return histograms_2D;
            };


            scope.$on('$destroy', function() {
                scope.dataSource.close();
            });
        }
	};
});