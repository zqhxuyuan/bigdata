app.factory('StreamMetadata', function() {

    function StreamMetadata(name, from, listedFeatures, namedFeatures) {
        this.name = name;
        this.from = from;
        this.listedFeatures = listedFeatures;
        this.namedFeatures = namedFeatures;
    }

    StreamMetadata.build = function (data) {
        return new StreamMetadata (
          data.name,
          data.from,
          data.listedFeatures,
          data.namedFeatures
        );
    };

    return StreamMetadata;
});

app.factory('Stream', function(StreamMetadata) {

    function Stream(metadata, name, selectedFeatures, mappedFeatures) {
        this.metadata = metadata;
        this.name = name;
        this.selectedFeatures = selectedFeatures;
        this.mappedFeatures = mappedFeatures;
    }

    Stream.build = function (metadata, data) {
        return new Stream(metadata, data.name, data.selectedFeatures, data.mappedFeatures);
    };

    Stream.newStream = function (metadata) {
        var selectedFeatures = {}
        for(var featureName in metadata.listedFeatures) {
            if (metadata.listedFeatures.hasOwnProperty(featureName)){
                selectedFeatures[featureName] = [];
            }
        }

        var mappedFeatures = {};
        for(var featureName in metadata.namedFeatures) {
            if (metadata.namedFeatures.hasOwnProperty(featureName)){
                mappedFeatures[featureName] = null;
            }
        }

        return new Stream(metadata, metadata.name, selectedFeatures, mappedFeatures);
    };

    Stream.newPreFilledStream = function (metadata) {
        var selectedFeatures = {}
        for(var featureName in metadata.listedFeatures) {
            if (metadata.listedFeatures.hasOwnProperty(featureName)){
                selectedFeatures[featureName] = [];
            }
        }

        var mappedFeatures = {};
        for(var featureName in metadata.namedFeatures) {
            if (metadata.namedFeatures.hasOwnProperty(featureName)){
                mappedFeatures[featureName] = featureName;
            }
        }

        return new Stream(metadata, metadata.name, selectedFeatures, mappedFeatures);
    };

    Stream.prototype.availableFeatures = function() {
        var features = [];
        $.each(this.mappedFeatures, function(name, value) {
            features.push(value);
        });

        $.each(this.selectedFeatures, function(name, values) {
           $.each(values, function(index, value) {
            features.push(value);
           });
        });

        return features;
    };

    Stream.prototype.mappedFeatureType = function(name) {
        return this.metadata.namedFeatures[name].capitalize();
    };

    Stream.prototype.selectedFeaturesType = function(name) {
        return this.metadata.listedFeatures[name].capitalize();
    };

    return Stream;
});