'use strict';

var app = angular.module('app', [ 'ngRoute', 'ngResource', 'ui.include', 'ui.bootstrap'])
	.config(function($routeProvider, $httpProvider) {
		$httpProvider.defaults.headers.common = {
			'Accept' : 'application/json',
			'Content-Type' : 'application/json'
		};

		$routeProvider.when('/pipelines', {
			templateUrl: 'views/pipeline/list.html',
			controller: 'ListPipelinesCtrl'
		}).when('/pipelines/:pipelineId', {
			templateUrl: 'views/pipeline/edit-pipeline.html',
			controller: 'EditPipelineCtrl'
		}).when('/clusters/:clusterId', {
			templateUrl: 'views/cluster/cluster.html',
			controller: 'ManageClusterCtrl'
		}).otherwise({
			redirectTo : '/pipelines'
		});
});
