app.factory('JsPlumbService', function(Connection) {
	
	var JsPlumbDiagram = function(containerId) {
		this.containerId = containerId;
		this.container = $("#" + containerId);
		
		this.jsPlumbInstance = jsPlumb.importDefaults({
			ConnectionsDetachable: false,
			// default drag options
			DragOptions : { cursor: 'pointer', zIndex: 2000, opacity: 0.20 },
			// the overlays to decorate each connection with.  note that the label overlay uses a function to generate the label text; in this
			// case it returns the 'labelText' member that we set on each connection in the 'init' method below.
			ConnectionOverlays : [
				[ "Arrow", { location:1 } ]
			],
			
			HoverPaintStyle : {strokeStyle:"#ec9f2e" },
			EndpointHoverStyle : {fillStyle:"#ec9f2e" },
			
			Container: containerId
		});
		
	};
	
	JsPlumbDiagram.prototype.bindPipelineConnections = function(pipeline) {
		var self = this;

		pipeline.connections.forEach(function(connection) {
			self.connect(connection);
		});
		
		this.jsPlumbInstance.bind("connection", function(info) {
			var from = {
                component: info.sourceId,
                stream: self.getOutputStreamName(info.sourceEndpoint._jsPlumb.uuid, info.sourceId)
            };
            var to = {
                component: info.targetId,
                stream: self.getInputStreamName(info.targetEndpoint._jsPlumb.uuid, info.targetId)
            };
			var connection = new Connection(from, to);

			pipeline.connections.push(connection);
		});
		
		this.jsPlumbInstance.bind("click", function(connection) {
			self.jsPlumbInstance.detach(connection); 
		});

		this.jsPlumbInstance.bind("connectionDetached", function(info) {
			var from = {
                component: info.sourceId,
                stream: self.getOutputStreamName(info.sourceEndpoint._jsPlumb.uuid, info.sourceId)
            };
            var to = {
                component: info.targetId,
                stream: self.getInputStreamName(info.targetEndpoint._jsPlumb.uuid, info.targetId)
            };
			var connection = new Connection(from, to);
			pipeline.connections.remove(connection);
		});
	};
	
	JsPlumbDiagram.prototype.connect = function(connection) {
		try {
			var fromUUID = this.getOutputStreamId(connection.from.component, connection.from.stream);
			var toUUID = this.getInputStreamId(connection.to.component, connection.to.stream);
			this.jsPlumbInstance.connect({uuids : [ fromUUID, toUUID ]});
		} catch (err) {
			console.log("Unable to create connection : %o", connection);
			console.log(err);
		}
		
	};
	
	JsPlumbDiagram.prototype.getOutputStreamId = function(id, streamName) {
		return id + "-out-" + streamName;
	};

	JsPlumbDiagram.prototype.getOutputStreamName = function(streamId, id) {
		return streamId.replace(id + "-out-", "");
	};

	JsPlumbDiagram.prototype.getInputStreamId = function(id, streamName) {
		return id + "-in-" + streamName;
	};

	JsPlumbDiagram.prototype.getInputStreamName = function(streamId, id) {
		return streamId.replace(id + "-in-", "");
	};
	
	JsPlumbDiagram.prototype.clearComponent = function(component) {
		this.jsPlumbInstance.removeAllEndpoints(component.id);
	};

	return {
		createDiagram : function(containerId) {
			return new JsPlumbDiagram(containerId);
		}
	};
});