app.directive('pipelineDataWorkflow', function($timeout, $modal, Component, ComponentMetadata, JsPlumbService) {
	var REDRAW_TIMEOUT = 50;

    return {
        restrict: "E",
		replace : true,
		templateUrl : 'views/pipeline/pipeline-data-workflow.html',
        scope: true,
        link: function (scope, element, attrs) {
            scope.$watch("pipeline", function(newValue, oldValue) {
                scope.displayPipeline(newValue);
                if(newValue != null && newValue.id != null) {
                    scope.validate();
                }
            });

            scope.displayPipeline = function(pipeline) {
                scope.diagram = JsPlumbService.createDiagram("jsplumb-container");

                if(pipeline != null && pipeline.id != null) {
                    setTimeout(function() {
                        scope.diagram.bindPipelineConnections(scope.pipeline);
                        scope.redraw();
                    }, REDRAW_TIMEOUT);
                }
            };
        
            scope.editComponent = function(component) {
                var modalInstance = $modal.open({
                    templateUrl : 'views/component/edit-component-modal.html',
                    controller : 'EditComponentCtrl',
                    resolve : {
                        component : function() {
                            return component;
                        }, pipeline: function() {
                            return scope.pipeline;
                        }
                    }
                });
        
                modalInstance.result.then(function(editedComponent) {
                    if (!editedComponent) {
                        scope.removeComponent(component);
                    } else {
                        console.log("'" + component.name + "' saved");
                    }
                    scope.validate();
                }, function(component) {
                    // Edition canceled
                });
            };
        
            scope.removeComponent = function(component) {
                scope.diagram.clearComponent(component);
                scope.pipeline.components.remove(component);
                scope.pipeline.removeConnectionsOf(component.id);
            };

            scope.addNewComponent = function(metadata, x, y) {
                var component = Component.newComponent(metadata);
                if(x) component.x = x;
                if(y) component.y = y;
                scope.pipeline.components.push(component);
                scope.redraw(true);
                scope.validate();
            };
        
            scope.redraw = function(deffered) {
                if (!deffered) {
                    scope.diagram.jsPlumbInstance.repaintEverything();
                } else {
                    setTimeout(function() {
                        scope.diagram.jsPlumbInstance.repaintEverything();
                    }, REDRAW_TIMEOUT);
                }
            };

            ComponentMetadata.findAll().then(function(metadatas) {
                scope.metadataByCategories = {};
                scope.metadataByIds = {};
                for(var id in metadatas) {
                    if (metadatas.hasOwnProperty(id)) {
                        var metadata = metadatas[id];
                        if(!scope.metadataByCategories.hasOwnProperty(metadata.category)) {
                            scope.metadataByCategories[metadata.category] = [];
                        }
                        scope.metadataByCategories[metadata.category].push(metadata);
                        scope.metadataByIds[metadata.id] = metadata;
                        scope.makeDraggable(metadata.id.toId());
                    }
                }
            });
        
            $("#add-component-panel").bind("dragover", function (e) {
                e.preventDefault();
                return false;
            });
        
            $("#add-component-panel").bind("dragenter", function dragEnter(e) {
                e.preventDefault();
                return true;
            });
        
            $("#add-component-panel").bind("drop", function (e) {
                var x = e.originalEvent.offsetX;
                var y = e.originalEvent.offsetY;
                var id = e.originalEvent.dataTransfer.getData("metadata-id");
                var metadata = scope.metadataByIds[id];
                scope.addNewComponent(metadata, x, y);
            });
        
            scope.makeDraggable = function(elementId) {
                $timeout(function(){
                    var element = $("#" + elementId);
                    element.attr('draggable', 'true');
                    element.attr('style', 'cursor:move;');
                    element.bind("dragstart", function(event){
                        event.originalEvent.dataTransfer.setData("metadata-id", element.attr("metadata-id"));
                        $("#add-component-panel").removeClass("ng-hide");
                        $("#jsplumb-panel").addClass("glow");
                    });
                    element.bind("dragend", function(event){
                        $("#add-component-panel").addClass("ng-hide");
                        $("#jsplumb-panel").removeClass("glow");
                    });
                }, 500);
            };
        }
    };
});