'use strict';

/* global suite, test */

var assert = require('assert');
var riakjs = require('..');

suite('fake-riak-js', function() {
  var db;
  test('riakjs.getClient', function(done) {
    db = riakjs.getClient({});
    assert(db instanceof riakjs.db);
    done();
  });
  test('db.save with secondary index', function(done) {
    db.save('bucket', 'id', {foo: 'bar'}, {index: {user: 'user'}}, function(err) {
      assert(!err);
      done();
    });
  });
  test('db.get', function(done) {
    db.get('bucket', 'id', function(err, value, meta) {
      assert(!err);
      assert(value.foo === 'bar');
      assert(meta.index.user === 'user');
      done();
    });
  });
  test('db.query', function(done) {
    db.query('bucket', {user: 'user'}, function(err, keys) {
      assert(!err);
      assert(Array.isArray(keys));
      assert(keys.length === 1);
      assert(keys[0] === 'id');
      done();
    });
  });
  test('db.mapreduce', function(done) {
    db.mapreduce.add({bucket: 'bucket', index: 'user_bin', key: 'user'})
      .map('Riak.mapValuesJson').run(function(err, values) {
        assert(!err);
        assert(Array.isArray(values));
        assert(values.length === 1);
        assert(values[0].foo === 'bar');
        done();
      });
  });
  test('db.remove', function(done) {
    db.remove('bucket', 'id', function(err) {
      assert(!err);
      done();
    });
  });
  test('db.get inexistant', function(done) {
    db.get('bucket', 'id', function(err, value, meta) {
      assert(err);
      assert(err instanceof Error);
      assert(err.notFound);
      assert(!value);
      assert(!meta);
      done();
    });
  });
  test('db.get inexistant', function(done) {
    db.get('bucket', 'id', function(err, value, meta) {
      assert(err);
      assert(err instanceof Error);
      assert(err.notFound);
      assert(!value);
      assert(!meta);
      done();
    });
  });
});
