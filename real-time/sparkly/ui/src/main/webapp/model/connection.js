app.factory('Connection', function() {

    function Connection(from, to) {
        this.from = from;
        this.to = to;
    };

    Connection.build = function (data) {
        return new Connection(
            data.from,
            data.to
        );
    };

    return Connection;
});