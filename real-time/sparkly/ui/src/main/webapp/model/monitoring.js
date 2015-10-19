app.factory('MonitoringMetadata', function($http) {

    function MonitoringMetadata(name, chartType, values, primaryValues, unit) {
        this.name = name;
        this.chartType = chartType;
        this.values = values;
        this.primaryValues = primaryValues;
        this.unit = unit;
    };

    MonitoringMetadata.build = function (data) {
        return new MonitoringMetadata(
            data.name,
            data.chartType,
            data.values,
            data.primaryValues,
            data.unit
        );
    };

    return MonitoringMetadata;
});

app.factory('Monitoring', function() {

    function Monitoring(metadata, active) {
        this.metadata = metadata;
        this.name = metadata.name;
        this.active = active;
    };

    Monitoring.build = function (metadata, data) {
        return new Monitoring (
            metadata,
            data.active
        );
    };


    Monitoring.newMonitoring = function(metadata) {
        return new Monitoring(metadata, true);
    };

    return Monitoring;
});

app.factory('MonitoringDataSource', function($http) {

    function MonitoringDataSource(master, componentId, monitoringName) {
        this.master = master;
        this.componentId = componentId;
        this.monitoringName = monitoringName;
        this.id = this.componentId + "-" + monitoringName.replace(/[^a-z0-9]/gmi, "-")  .toLowerCase();
        this.sockets = [];
    }

    MonitoringDataSource.prototype.listen = function(onData, onPastData) {
        var self = this;
        var socket = $.atmosphere;
        this.sockets.push(socket);
        var request = {
            url: 'api/monitoring/data/' + this.master + '/' + this.id,
            logLevel: 'info',
            transport: 'websocket'
        };
        request.onMessage = function(message) {
            var json = jQuery.parseJSON(message.responseBody);
            onData(json);
        };
        request.onOpen = function(message) {
            request.transport = message.transport;
        };

        if(onPastData) {
            $http.get('api/monitoring/past_data/' + this.master + '/' + this.id).then(function(pastData) {
                onPastData(pastData.data);
                socket.subscribe(request);
            });
        } else {
            socket.subscribe(request);
        }
    }


    MonitoringDataSource.prototype.close = function() {
        $.each(this.sockets, function(index, socket) {
            socket.unsubscribe();
        });
    }

    return MonitoringDataSource;
});