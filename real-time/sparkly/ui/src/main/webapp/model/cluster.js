app.factory('ClusterStatus', function(Pipeline) {

    function ClusterStatus(state, time, pipeline) {
        this.state = state;
        this.time = time;
        this.pipeline = pipeline;
    }

    ClusterStatus.build = function (data) {
        var pipeline = data.pipeline ? Pipeline.build(data.pipeline) : null;
        return new ClusterStatus(data.state, data.time, pipeline);
    };

    ClusterStatus.prototype.prettyString = function() {
        switch(this.state) {
            case 'Stopped':
                return 'Stopped';
            case 'Deploying':
                return 'Deploying ' + this.pipeline.name;
            case 'Running':
                return 'Running ' + this.pipeline.name;
            case 'Stopping':
                return 'Stopping';
            default:
                return '';

        }
    };

    return ClusterStatus;
});

app.factory('Cluster', function(ClusterStatus, $http) {

    function Cluster(id, name, status) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.monitorings = [];
    }

    Cluster.build = function (data) {
        return new Cluster(data.id, data.name, ClusterStatus.build(data.status));
    };

    Cluster.findById = function(id) {
        return $http.get('api/clusters/' + id).then(function(cluster) {
            return Cluster.build(cluster.data);
        });
    };

    Cluster.prototype.deploy = function(pipelineId) {
        return $http.post('api/clusters/' + this.id  + "/deploy?pipelineId=" + pipelineId);
    };

    Cluster.prototype.restart = function(pipelineId) {
        return $http.post('api/clusters/' + this.id  + "/restart?pipelineId=" + pipelineId);
    };

    Cluster.prototype.stop = function(pipelineId) {
        return $http.post('api/clusters/' + this.id  + "/stop");
    };

    Cluster.prototype.isRunning = function() {
        return this.status.state == "Running";
    };

    Cluster.prototype.isReadyToDeploy = function() {
        return this.status.state == "Running" || this.status.state == "Stopped";
    };

    Cluster.prototype.updateStatus = function() {
        var self = this;
        $http.get('api/clusters/' + self.id + '/status').then(function(status) {
            self.status = ClusterStatus.build(status.data);
            if(self.status.pipeline != null && self.monitorings.length != self.status.pipeline.activeMonitorings().length) {
                self.monitorings = self.status.pipeline.activeMonitorings();
            }
        });
    };

    return Cluster;
});
