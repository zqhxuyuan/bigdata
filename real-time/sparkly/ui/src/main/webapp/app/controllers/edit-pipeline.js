app.controller('EditPipelineCtrl', function($scope, $location, $routeParams, $modal, $timeout,
		NotificationService, JsPlumbService, Pipeline, Component, ComponentMetadata, ValidationReport, Cluster) {

    $scope.currentView = "Data workflow";

	$scope.pipeline = {};
	$scope.validationReport = new ValidationReport([]);

	$scope.validate = function() {
	    Pipeline.validate($scope.pipeline).then(function(report) {
    	    $scope.validationReport = report;
        }, function(error) {
            NotificationService.notify("Unable to validate pipeline", "danger");
        });
	};

	$scope.save = function() {
		$scope.pipeline.save().then(function(data) {
			NotificationService.notify("'" + $scope.pipeline.name + "' saved");
		}, function(error) {
			NotificationService.notify("'" + $scope.pipeline.name + "' not saved! ", "danger");
		});
	};

    Pipeline.findById($routeParams.pipelineId).then(function(pipeline) {
	    $scope.pipeline = pipeline;
    }, function(error) {
        NotificationService.notify("Unable to load pipeline", "danger");
        $location.path("pipelines/");
    });

    Cluster.findById("local").then(function(cluster) {
        $scope.cluster = cluster;
    });

    $scope.saveAndLaunch = function() {
		$scope.pipeline.save().then(function(data) {
			NotificationService.notify($scope.pipeline.name + " saved");
			$scope.launch("local");
		}, function(error) {
                NotificationService.notify("Unable to deploy pipeline " + $scope.pipeline.name + " because it could not be saved.", "danger");
		});
    };

    $scope.launch = function(clusterId) {
        Cluster.findById("local").then(function(cluster) {
            NotificationService.notify("Deploying pipeline on " + cluster.name);
            $location.path('clusters/' + clusterId).replace();
            cluster.deploy($scope.pipeline.id).then(function(success) {
                NotificationService.notify($scope.pipeline.name + " deployed on " + cluster.name);
            }, function(error) {
                NotificationService.notify("Unable to deploy pipeline " + $scope.pipeline.name + " on " + cluster.name, "danger");
            });
        });
    };
});