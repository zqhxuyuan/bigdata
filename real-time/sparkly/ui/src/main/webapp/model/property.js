app.factory('PropertyMetadata', function() {

    function PropertyMetadata(name, defaultValue, propertyType, mandatory, acceptedValues) {
        this.name = name;
        this.defaultValue = defaultValue ? defaultValue.toString() : null;
        this.propertyType = propertyType;
        this.mandatory = mandatory;
        this.acceptedValues = acceptedValues;
    }

    PropertyMetadata.build = function (data) {
        return new PropertyMetadata (data.name, data.defaultValue, data.propertyType, data.mandatory, data.acceptedValues);
    };

    PropertyMetadata.prototype.hasLimitedValues = function() {
        return this.acceptedValues != null && this.acceptedValues.length > 0;
    };

    return PropertyMetadata;
});

app.factory('Property', function() {

    function Property(metadata, name, value) {
        this.metadata = metadata;
        this.name = name;
        this.value = value;
    }

    Property.build = function (metadata, data) {
        return new Property(metadata, data.name, data.value);
    };

    Property.newProperty = function (metadata) {
        return new Property(metadata, metadata.name, metadata.defaultValue);
    };

    return Property;
});