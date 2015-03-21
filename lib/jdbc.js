var _ = require('underscore');
var java = require('java');
java.options.push('-Xrs');

function trim1 (str) {
  return (str || '').replace(/^\s\s*/, '').replace(/\s\s*$/, '');
}

var isNumber = {};

[-7, -6, 5, 4, -5, 6, 7, 8, 2, 3].forEach(function(idx) {
  isNumber[idx] = true;
});


function JDBCConn() {
  this._config = {};
  this._conn = null;
}

JDBCConn.prototype.initialize = function(config, callback) {
  var self = this;
  self._config = config;
  self.java = java;
  
  if (self._config.libpath) {
    java.classpath.push(self._config.libpath);
  }
  if (self._config.libs) {
   java.classpath.push.apply(java.classpath, self._config.libs); 
  }
  
  java.newInstance(self._config.drivername, function(err, driver) {
    if (err) {
      return callback(err);
    } else {
      java.callStaticMethod('java.sql.DriverManager','registerDriver', driver, function(err, result) {
        if (err) {
          return callback(err);
        } else {
          return callback(null, self._config.drivername);
        }
      });
    }
  });
};

JDBCConn.prototype.open = function(callback) {
  var self = this;

  if(self._config.user || self._config.password) {
    java.callStaticMethod('java.sql.DriverManager', 'getConnection', self._config.url, self._config.user, self._config.password, function (err, conn) {
      if (err) {
        return callback(err);
      } else {
        self._conn = conn;
        return callback(null, conn);
      }
    });
  } else {
    java.callStaticMethod('java.sql.DriverManager', 'getConnection', self._config.url, function (err, conn) {
      if (err) {
        return callback(err);
      } else {
        self._conn = conn;
        return callback(null, conn);
      }
    });
  }
};

JDBCConn.prototype.close = function(callback) {
  var self = this;

  if (self._conn) {
    self._conn.close(function(err) {
      if (err) {
        return callback(err);
      } else {
        self._conn = null;
        return callback(null);
      }
    });
  }
};

JDBCConn.prototype.executeQuery = function(sql, callback) {
  var self = this;

  self._conn.createStatement(function(err, statement) {
    if (err) {
      return callback(err);
    } else {
      statement.executeQuery(sql, function(err,resultset) {
        self.processResultSet(err, resultset, callback)
      });
    }
  });
};

JDBCConn.prototype.processResultSet = function(err, resultset, callback) {
  var self = this;

  if (err) {
    return callback(err);
  } else if (resultset) {
    resultset.getMetaData(function(err,rsmd) {
      if (err) {
        return callback(err);
      } else {
        var results = [];
        var cc = rsmd.getColumnCountSync();
        var columns = [''];
        for(var i = 1; i <= cc; i++) {
          var colname = rsmd.getColumnNameSync(i);
          columns.push(colname);
        }
        var next = resultset.nextSync();
        var processRow = function(next){
          if(next){
            setImmediate(function(){
              var row = {};
              for(var a= 1; a <= cc; a++) {
                
                var colType = rsmd.getColumnTypeSync(a);                
                var rawValue;
                
                if (isNumber[colType]) {
                  rawValue = Number(resultset.getStringSync(a));
                } else if (colType === 91) {
                  rawValue = new Date(resultset.getDateSync(a).getTimeSync());
                } else if (colType === 92) {
                  rawValue = new Date(resultset.getTimeSync(a).getTimeSync());
                } else if (colType === 93) {
                  rawValue = new Date(resultset.getTimestampSync(a).getTimeSync());
                } else if (colType === 16) {
                  rawValue = resultset.getBooleanSync(a);
                } else {
                  rawValue = resultset.getStringSync(a);
                }
                
                row[columns[a]] = resultset.wasNullSync() ? null : (typeof rawValue === 'string' ? trim1(rawValue) : rawValue);
              }
              results.push(row);
              next = resultset.nextSync();
              processRow(next);
            });
          } else {
            callback(null, results);
          }
        };
        processRow(next);
      }
    });
  } else {
    return callback(null, null);
  }
};

JDBCConn.prototype.executeUpdate = function(sql, callback) {
  var self = this;

  self._conn.createStatement(function(err, statement) {
    if (err) {
      return callback(err);
    } else {
      statement.executeUpdate(sql, function(err, rowcount) {
        if (err) {
          return callback(err);
        } else {
          return callback(null, rowcount);
        }
      });
    }
  });
};

module.exports = JDBCConn;
