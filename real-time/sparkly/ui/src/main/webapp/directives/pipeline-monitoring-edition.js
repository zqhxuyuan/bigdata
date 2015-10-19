app.directive('pipelineMonitoringEdition', function() {
    return {
        restrict: "E",
		replace : true,
		templateUrl : 'views/pipeline/pipeline-monitoring-edition.html',
        scope : {
            monitoring : "=monitoring"
        },
        link: function (scope, element, attrs) {
            var input = $(element).children("input").first();

            input.attr("checked", scope.monitoring.active);
            input.bootstrapSwitch();

            input.on('switchChange.bootstrapSwitch', function(e, data) {
                scope.monitoring.active = data;
            });
        }
    };
});