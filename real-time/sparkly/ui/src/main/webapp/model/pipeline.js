app.factory('Pipeline', function($http, Component, Connection, ValidationReport) {

    function Pipeline(id, name, description, batchDurationMs, components, connections, settings) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.batchDurationMs = batchDurationMs;
        this.components = components;
        this.connections = connections;
        this.settings = settings;
    };

    Pipeline.build = function (data) {
        return new Pipeline(
            data.id,
            data.name,
            data.description,
            data.batchDurationMs,
            data.components.map(Component.build),
            data.connections.map(Connection.build),
            data.settings
        );
    };

    Pipeline.findAll = function() {
        return $http.get('api/pipelines/').then(function(pipelines) {
            return $.map(pipelines.data, Pipeline.build);
        });
    };

    Pipeline.findById = function(id) {
        return $http.get('api/pipelines/' + id).then(function(pipeline) {
            return Pipeline.build(pipeline.data);
        });
    };

    Pipeline.delete = function(id) {
        return $http.delete('api/pipelines/' + id);
    };

    Pipeline.create = function(name) {
        return $http.post('api/pipelines/' + name).then(function(pipeline) {
            return Pipeline.build(pipeline.data);
        });
    };

    Pipeline.prototype.save = function() {
        return $http.put('api/pipelines/' + this.id, this);
    };

    Pipeline.validate = function(pipeline) {
        return $http.post('api/pipeline-validation', pipeline).then(function(report) {
            return ValidationReport.build(report.data);
        });
    };

    Pipeline.prototype.retrieveAvailableFeatures = function(component, stream) {
        var self = this;
        var features = [];
        var connections = this.connectedTo(component.id, stream.name);

        connections.forEach(function(connection) {
            var otherComponent = self.component(connection.from.component);
            var os = otherComponent.outputStream(connection.from.stream);

            var osFeatures = os.availableFeatures();
            features.pushAll(osFeatures);

            if(os.metadata.from) {
                var is = otherComponent.inputStream(os.metadata.from)
                features.pushAll(self.retrieveAvailableFeatures(otherComponent, is));
            }
        });

        return features;
    };

    Pipeline.prototype.connectedTo = function(componentId, streamName) {
        var self = this;
        var connectionsTo = [];

        this.connections.forEach(function (connection) {
            if(connection.to.component == componentId && connection.to.stream == streamName) {
                connectionsTo.push(connection);
            }
        });

        return connectionsTo;
    };

    Pipeline.prototype.removeConnectionsOf = function(componentId) {
        this.connections = $.grep(this.connections, function (connection) {
            return connection.to.component != componentId && connection.from.component != componentId;
        });
    };

    Pipeline.prototype.component = function(componentId) {
        return $.grep(this.components, function (component) { return component.id == componentId})[0];
    };

    Pipeline.prototype.activeMonitorings = function() {
        var results = [];
        for(var i = 0; i < this.components.length; i++ ) {
            var component = this.components[i];
            for(var j = 0; j < component.monitorings.length; j ++) {
                if(component.monitorings[j].active) {
                    results.push({metadata: component.monitorings[j].metadata, component: component});
                }
            }
        }
        return results;
    };

    return Pipeline;
});

app.factory('ValidationReport', function(ValidationMessage) {

    function ValidationReport(messages) {
        this.messages = messages;
        this.errorCount = $.grep(this.messages, function (message) { return message.level == "Error"}).length;
        this.warningCount = $.grep(this.messages, function (message) { return message.level == "Warning"}).length;
    }

    ValidationReport.build = function (data) {
        var messages = data.messages.map(ValidationMessage.build)
        return new ValidationReport(messages);
    };

    ValidationReport.prototype.isEmpty = function() {
        return this.messages.length <= 0;
    };

    ValidationReport.prototype.size = function() {
        return this.messages.length;
    };

    return ValidationReport;
});


app.factory('ValidationMessage', function() {

    function ValidationMessage(level, text) {
        this.level = level;
        this.text = text;
    }

    ValidationMessage.build = function (data) {
        return new ValidationMessage(data.level, data.text);
    };

    return ValidationMessage;
});
