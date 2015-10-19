app.controller('ListPipelinesCtrl', function ($scope, $location, $modal, Pipeline) {
	
	$scope.pipelines = [];
	
	$scope.refreshPipelines = function () {
	    Pipeline.findAll().then(function(pipelines) {
            $scope.pipelines = pipelines;
        });
	};
	

    $scope.deletePipeline = function() {
    	var id = this.pipeline.id;
    	Pipeline.delete(id).then(function() {
            $scope.refreshPipelines();
            console.log("Deleted " + id);
		}, function() {
            console.log("Failing deletion of " + id);
		});
    };

    $scope.openCreationModal = function() {
        var modalInstance = $modal.open({
            templateUrl : 'views/pipeline/create-pipeline-modal.html',
            controller : 'CreatePipelineCtrl',
            resolve: {
                info: function() { return {'name': ''}; }
            }
        });

        modalInstance.result.then(function(info) {
            if (info) {
                Pipeline.create(info.name).then(function(pipeline) {
                    $location.path("pipelines/" + pipeline.id);
                });
            }
        });
    };
	
	$scope.refreshPipelines();
});