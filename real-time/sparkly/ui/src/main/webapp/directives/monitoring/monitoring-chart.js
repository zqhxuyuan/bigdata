app.directive('monitoringChart', function($compile) {

    return {
        restrict: "E",
        scope : {
            clusterid : "=",
            componentid : "=",
            componentname : "=",
            monitoring: "="
        },
        link: function(scope, element, attrs){
            var tag = scope.monitoring.chartType.replace(/_/g, "-") + "-monitoring-chart";

            var e = $compile("<" + tag + " clusterid='clusterid' monitoring='monitoring' componentid='componentid' componentname='componentname'></" + tag + ">")(scope);
            element.replaceWith(e);
        }
    };

});