app.directive('timeSeriesChart', function(MonitoringDataSource) {
	return {
		restrict : "E",
		replace : true,
		templateUrl : 'views/monitoring/time-series-chart.html',
		scope : {
			control: "="
		},
		link : function(scope, element, attrs) {

            scope.cleanOptions = function(options) {
                options.title = options.title || '';
                options.type = options.type || 'line';
                options.unstack = options.unstack || false;
                options.series_names = options.series_names || [];
                options.value_names = options.value_names || [];
                options.unit = options.unit || '';
                options.y_range = typeof options.y_range === 'undefined' ? null : options.y_range;
                options.legend = typeof options.legend === 'undefined' ? true : options.legend;
                options.shelving = typeof options.shelving === 'undefined' ? true : options.shelving;
                options.x_axis = typeof options.x_axis === 'undefined' ? true : options.x_axis;
                options.y_axis = typeof options.y_axis === 'undefined' ? true : options.y_axis;
                options.hover =  typeof options.hover === 'undefined' ? true : options.hover;

                return options;
            }

            scope.init = function(options, data) {
                scope.options = scope.cleanOptions(options);

                scope.values = [];
                var palette = new Rickshaw.Color.Palette();
                scope.series = scope.options.series_names.map(function(name){return{name: name, color: palette.color()};});

                scope.timeBase = data[0] == null ? Date.now() / 1000 : data[0].timestamp / 1000;
                scope.graph = new Rickshaw.Graph({
                    element: element.context.querySelector('.chart'),
                    renderer: scope.options.type,
                    stroke: true,
                    height: 300,
                    preserve: true,
                    interpolation: 'linear',
                    series: new Rickshaw.Series.FixedDuration(scope.series, undefined, {
                        timeInterval: 1000, // TODO : fixed slideDUration
                        maxDataPoints: 60,
                        timeBase: scope.timeBase
                    })
                });

                if(scope.options.y_range) {
                    scope.graph.min = scope.options.y_range[0];
                    scope.graph.max = scope.options.y_range[1];
                }

                if(scope.options.unstack) {
                    scope.graph.renderer.unstack = true;
                    scope.graph.renderer.offset = 'zero';
                }

                if(scope.options.hover) {
                    var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                        graph: scope.graph,
                        yFormatter: function(y){
                          return y + " " + scope.options.unit;
                        },
                        xFormatter: function(x) {return new Date(x * 1000).toString();}
                    });
                }

                if(scope.options.legend) {
                    var legend = new Rickshaw.Graph.Legend( {
                        graph: scope.graph,
                        element: element.context.querySelector('.chart-legend')
                    });

                    if(scope.options.shelving) {
                        var shelving = new Rickshaw.Graph.Behavior.Series.Toggle({
                            graph: scope.graph,
                            legend: legend
                        });
                    }
                }

                if(scope.options.x_axis) {
                    var xAxis = new Rickshaw.Graph.Axis.X( {
                        graph: scope.graph,
                        ticksTreatment: 'glow',
                        ticks: 5,
                        tickFormat: function(d) {return d3.time.format("%H:%M:%S")(new Date(d * 1000));}
                    } );
                    xAxis.render();
                }

                if(scope.options.y_axis) {
                    var yAxis = new Rickshaw.Graph.Axis.Y({
                        graph: scope.graph,
                        tickFormat: function(y){
                            return y + " " + scope.options.unit;
                        },
                        ticksTreatment: 'glow',
                        ticks: 2,
                    });
                    yAxis.render();
                }

                scope.addAll(data);
            };

           scope.add = function(event, render) {
                // Upate series
                var data = {};
                for (var i = 0; i < scope.options.series_names.length; i++) {
                    var name = scope.options.series_names[i];
                    if(event.data[name]) {
                        data[name] = event.data[name];
                    }
                }

                var time = Math.round(event.timestamp / 1000);
                scope.graph.series.addData(data, time);

                // Update values
                scope.values = [];
                for (var i = 0; i < scope.options.value_names.length; i++) {
                    var name = scope.options.value_names[i];
                    var value = event.data[name];
                    scope.values.push({name: name, value: value});
                }

                if(render) {
                    scope.graph.render();
                }
            };

            scope.addAll = function(events) {
                 $(events).each(function(index, event){scope.add(event, false);});
                 scope.graph.render();
            };

            scope.replaceData = function(data) {
                scope.values = [];
                scope.graph.element.removeAllListeners();
                $(scope.element.context.querySelector('.chart')).empty();
                $(scope.element.context.querySelector('.chart-legend')).empty();
                scope.init(scope.options, data);
            };

            scope.element = element;
            scope.options = scope.cleanOptions({});

            scope.values = [];
            scope.series = [];

            scope.control.init = function(options, data) { scope.init(options, data)};
            scope.control.add = function(event) {scope.add(event, true);};
            scope.control.addAll = function(events) {scope.addAll(events);};
            scope.control.clearData = function() { scope.replaceData([])};
            scope.control.replaceData = function(events) { scope.replaceData(events)};
            scope.control.title = function(title) { scope.options.title = title};
        }
	};
});