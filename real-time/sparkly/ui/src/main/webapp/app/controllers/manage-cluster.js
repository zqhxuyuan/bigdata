app.controller('ManageClusterCtrl', function($scope, $route, $location, $routeParams, $timeout,
    NotificationService, Cluster, Pipeline, MonitoringMetadata) {

	$scope.cluster = {};
	$scope.clusterId = $routeParams.clusterId;
	$scope.monitorings = [];

	$scope.memoryMonitoring = new MonitoringMetadata("Memory", "VALUES", ['Memory used', 'Memory committed', 'Max memory'], ['Memory used'], "GB");
	$scope.latencyMonitoring = new MonitoringMetadata("Latency", "STACKED_VALUES", ["Scheduling delay", "Processing delay"], ["Total delay"], "ms");

    Cluster.findById($scope.clusterId).then(function(cluster) {
        $scope.cluster = cluster;
        $scope.updateStatus();
    }, function(error) {
        NotificationService.notify("Unable to load cluster " + $routeParams.clusterId, "danger");
        $location.path("/");
    });

	$scope.pipelines = [];
    Pipeline.findAll().then(function(pipelines) {
        $scope.pipelines = pipelines;
    });

    $scope.deploy = function(pipeline) {
        $scope.cluster.deploy(pipeline.id).then(function(success) {
            NotificationService.notify("Pipeline " + pipeline.name + " deployed on " + $scope.cluster.name);
         }, function(error) {
            NotificationService.notify("Unable to deploy pipeline " + pipeline.name + " on " + $scope.cluster.name, "danger");
         });
    };

    $scope.restart = function(pipeline) {
        $scope.cluster.restart(pipeline.id).then(function(success) {
            NotificationService.notify("Pipeline " + pipeline.name + " restarted on " + $scope.cluster.name);
         }, function(error) {
            NotificationService.notify("Unable to restart pipeline " + pipeline.name + " on " + $scope.cluster.name, "danger");
         });
    };

    $scope.updateStatus = function() {
        $scope.cluster.updateStatus();
        $scope.updateStateCss();
        $timeout($scope.updateStatus, 1000);
    }

    $scope.stateCss = "warning";
    $scope.updateStateCss = function() {
        switch($scope.cluster.status.state) {
            case "Running":
                $scope.stateCss = "success";
                break;
            case "Stopped":
                $scope.stateCss = "danger";
                break;
            default :
                $scope.stateCss = "warning"
                break;
        }
    }
});