'use strict';

var notFoundError = function() {
  var err = new Error('Not found');
  err.notFound = true;
  return err;
};

var db = function() {
  this.buckets = {};
  this.mapreduce = {
    add: this.add.bind(this)
  };
};
db.prototype.save = function(bucket, id, data, meta, callback) {
  callback = callback || function() {};

  if (!this.buckets[bucket]) {
    this.buckets[bucket] = {
      values: {},
      indexes: {}
    };
  }

  this.buckets[bucket].values[id] = {data: data, meta: meta, id: id};
  //secondary index
  if (meta.index) {
    for (var i in meta.index)
      this.buckets[bucket].indexes[i + ':' + meta.index[i]] = this.buckets[bucket].values[id];
  }

  callback();
};
db.prototype.remove = function(bucket, id, callback) {
  callback = callback || function() {};

  bucket = this.buckets[bucket];
  if (!bucket)
    return callback(notFoundError());

  if (bucket.values[id]) {
    var idx = bucket.values[id].meta.index;
    if (idx) {
      for (var i in idx) {
        delete bucket.indexes[i + ':' + idx[i]];
      }
    }
    delete bucket.values[id];
    callback();
  }
  else
    callback(notFoundError());
};
db.prototype.get = function(b, id, callback) {
  callback = callback || function() {};

  var bucket = this.buckets[b];
  if (!bucket)
    return callback(notFoundError());

  var value = bucket.values[id];
  if (!value)
    return callback(notFoundError());

  var data;
  if (typeof value.data === 'object' && !Buffer.isBuffer(value.data))
    data = JSON.parse(JSON.stringify(value.data));
  else
    data = value.data;

  callback(null, data, value.meta);
};
db.prototype.query = function(bucket, query, callback) {
  callback = callback || function() {};

  var keys = [];
  bucket = this.buckets[bucket];
  if (bucket)
    for (var i in query) {
      var value = bucket.indexes[i + ':' + query[i]];
      if (value)
        keys.push(value.id);
    }
  callback(null, keys);
};

db.prototype.add = function(data) {
  var self = this;
  this.map = function() {
    this.run = function(callback) {
      callback = callback || function() {};
      var res = [];
      var b = data.bucket;
      var k = data.key;
      var i = data.index.split('_')[0];
      var bucket = self.buckets[b];
      for (var y in bucket.indexes) {
        var t = y.split(':');
        if (t[0] === i && t[1] === k)
          res.push(bucket.indexes[y].data);
      }

      callback(null, res);
    };
    return this;
  };
  return this;
};

module.exports = {
  getClient: function() {
    return new db();
  },
  db: db,
  notFoundError: notFoundError
};