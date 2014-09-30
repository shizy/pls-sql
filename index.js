var mssql = require('mssql'),
    fs    = require('fs');

var Sql = module.exports = function Sql(type, configFile) {

   this.type = type || 'mssql';
   this.file = configFile || __dirname + '/connections.json';
   this.json = JSON.parse(fs.readFileSync(this.file));
   this.conn = null;
   this.req  = null;
};

function prepare (query, input) {

    input.forEach(function (i) {
        query = query.replace(/[?]/, "'" + i + "'"); // support for int, string, etc.
    });

    return query;
}

Sql.prototype.Open = function (title) {

    switch(this.type) {

        case 'mssql':
            this.conn = new mssql.Connection(this.json[title], function (err) {
                if (err) { return err; }
            });
            break;
    }
};

Sql.prototype.Query = function (query, inputParams, cb) {

    query = fs.existsSync(query) ? fs.readFileSync(query).toString() : query;

    if (typeof inputParams === 'function' || typeof inputParams === 'undefined') {
        cb = inputParams;
    } else {
        query = prepare(query, inputParams);
    }

    switch(this.type) {

        case 'mssql':
            var req = new mssql.Request(this.conn);

            if (typeof cb === 'function') {
                req.query(query, function (err, rec) {
                    if (err) { return cb(err); }
                    cb(null, rec);
                });
            } else  {
                this.req = req;
                this.req.stream = true;
                this.req.query(query);
            }
            break;
    }
};

// Callback (error, done?, row)
Sql.prototype.Results = function (cb) {

    switch(this.type) {

        case 'mssql':
            this.req.on('row', function (row) {
                cb(null, false, row);
            });
            this.req.on('done', function (ret) {
                cb(null, true);
            });
            this.req.on('error', cb);
            break;
    }
};

Sql.prototype.Close = function () {

    switch(this.type) {

        case 'mssql':
            this.conn.close();
            break;
    }
};
