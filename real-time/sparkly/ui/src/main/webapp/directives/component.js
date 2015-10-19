app.directive('component', function() {    
	var COMPONENT_BASE_SIZE = 20;
	var ENDPOINT_SIZE = 20;
	
    return {
        restrict: "E",
        replace : true,
        template: '<div class="diagram-component"><div class="diagram-component-title" ng-click="onTitleClicked(model)">{{model.name}}</div></div>',
        scope: {
            model: "=",
            onTitleClicked: '&ngClick'
        },
        link: function (scope, element, attrs) {
        	var component = scope.model;
        	var metadata = component.metadata;
        	var diagram = scope.$parent.diagram;
        	
        	element.attr("id", component.id);
        	element.css('top', component.y);
        	element.css('left', component.x);

        	element.draggable({
        		drag : function() {
        			diagram.jsPlumbInstance.repaint(component.id);
        			scope.$apply(function read() {
        				component.x = parseInt(element.css('left'), 10);
        				component.y = parseInt(element.css('top'), 10);
        			});
        		}, stop: function() {
        			diagram.jsPlumbInstance.repaint(component.id);
        		}
        	});

			var inputsCount = metadata.inputs.length;
			var outputsCount = metadata.outputs.length;
			element.css("height", COMPONENT_BASE_SIZE + Math.max(inputsCount, outputsCount) * ENDPOINT_SIZE);

			// Create endpoints for input streams
			for(var i = 0; i < inputsCount; i++) {
				var inputStream = metadata.inputs[i];
				var yOffset =  (i + 1) / (inputsCount + 1);
				var endpointUUID = diagram.getInputStreamId(component.id, inputStream.name);
				var e = diagram.jsPlumbInstance.addEndpoint(element, {
					uuid: endpointUUID,
					isTarget: true,
					maxConnections: 1,
					overlays:[ [ "Label", { label: inputStream.name, location: [2, 0.5], cssClass: "diagram-input-label"}]],
					anchor : [ [ 0, yOffset, -1, 0] ]
				});
				e.canvas.id = endpointUUID;
			}
			
			// Create endpoints for output streams
			for(var i = 0; i < outputsCount; i++) {
				var outputStream = metadata.outputs[i];
				var yOffset =  (i + 1) / (outputsCount + 1);
				var endpointUUID =  diagram.getOutputStreamId(component.id, outputStream.name);
				var e = diagram.jsPlumbInstance.addEndpoint(element, {
					uuid:  endpointUUID,
					isSource: true,
					maxConnections: -1,
					overlays:[ [ "Label", { label: outputStream.name, location: [-1, 0.5], cssClass: "diagram-output-label"}]],
					anchor : [ [ 1, yOffset, 1, 0] ]
				});
				e.canvas.id = endpointUUID;
			}
        }
    };
});