var amqp = require('amqplib/callback_api');
var async = require('async');
var crypto = require('crypto');
var java = require('java');
var mysql = require('mysql');
var redis = require('redis'),
    redisClient = redis.createClient('redis://host:port');

var QUEUE_NAME = 'oqa_mobile_log_queue';

function listen(ch, pool) {
    ch.assertQueue(QUEUE_NAME, {
        durable: true
    });
    ch.consume(QUEUE_NAME, function(msg) {
        pool.getConnection(function(err, connection) {
            if (!err) {
                var j = JSON.parse(msg.content.toString());
                async.waterfall([
                    function(callback) {
                        redisClient.hget('hqa_projects', j.exception.apikey, function(err, pid) {
                            if (err) {
                                callback(err, null);
                            } else {
                                if (pid == null) {
                                    callback('Project not exists', null);
                                } else {
                                    callback(null, pid);
                                }
                            }
                        });
                    },
                    function(pid, callback) {
                        var hash = crypto.createHash('md5').update(j.exception.callstack).digest('hex');
                        connection.query('SELECT * FROM errors where callstack_key=? and pid=?', [hash, pid], function(err, rows) {
                            if (err) {
                                callback(err, null);
                            } else {
                                if (rows.length >= 1) {
                                    // UPDATE
                                    callback(null, pid, rows[0], null);
                                } else {
                                    // INSERT
                                    callback(null, pid, null, hash);
                                }
                            }
                        });
                    },
                    function(pid, row, hash, callback) {
                        if (row != null) {
                            connection.query('UPDATE errors SET numofinstances=?,wifion=?,mobileon=?,gpson=?,totalmemusage=?,lastdate=NOW() WHERE iderror=?', [row.numofinstances + 1, row.wifion + j.exception.wifion, row.mobileon + j.exception.mobileon, row.gpson + j.exception.gpson, row.totalmemusage + j.exception.appmemtotal, row.iderror], function(err, result) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, pid, row.iderror);
                                }
                            });
                        } else {
                            connection.query('INSERT INTO errors (callstack_key, callstack, rank, numofinstances, errorname, errorclassname, linenum, status, createdate, lastdate, wifion, mobileon, gpson, totalmemusage, pid) VALUES (?,?,?,?,?,?,?,?,NOW(),NOW(),?,?,?,?,?)', [hash, j.exception.callstack, j.exception.rank, 1, j.exception.errorname, j.exception.errorclassname, j.exception.linenum, 0, j.exception.wifion, j.exception.mobileon, j.exception.gpson, j.exception.appmemtotal, pid], function(err, result) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, pid, result.insertId);
                                }
                            });
                        }
                    },
                    function(pid, errorid, callback) {
                        connection.query('INSERT INTO instances (sdkversion, locale, datetime, device, country, appversion, osversion, gpson, wifion, mobileon, scrwidth, scrheight, batterylevel, availsdcard, rooted, appmemtotal, appmemfree, appmemmax, kernelversion, xdpi, ydpi, scrorientation, sysmemlow, lastactivity, carrier_name, iderror, pid) VALUES (?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [j.exception.sdkversion, j.exception.locale, j.exception.device, j.exception.country, j.exception.appversion, j.exception.osversion, j.exception.gpson, j.exception.wifion, j.exception.mobileon, j.exception.scrwidth, j.exception.scrheight, j.exception.batterylevel, j.exception.availsdcard, j.exception.rooted, j.exception.appmemtotal, j.exception.appmemfree, j.exception.appmemmax, j.exception.kernelversion, j.exception.xdpi, j.exception.ydpi, j.exception.scrorientation, j.exception.sysmemlow, j.exception.lastactivity, j.exception.carrier_name, errorid, pid], function(err, result) {
                            if (err) {
                                callback(err, null);
                            } else {
                                callback(null, pid, errorid, result.insertId);
                            }
                        });
                    },
                    function(pid, errorid, iid, callback) {
                        connection.query('INSERT INTO console_log (console_log, datetime, idinstance) VALUES (?,NOW(),?)', [j.console_log.data, iid], function(err, result) {
                            if (err) {
                                callback(err, null);
                            } else {
                                callback(null, null);
                            }
                        });
                    }
                ], function(err, result) {
                    connection.release();
                    if (err) {
                        console.log(err);
                    }
                })
            }
        });
    }, {
        noAck: true
    });
}

amqp.connect('amqp://id:pw@host:5672', function(err, conn) {
    if (err != null) {
        console.error(err);
        process.exit(1);
    }
    conn.createChannel(function(err, ch) {
        if (err != null) {
            console.error(err);
            process.exit(1);
        }
        var pool = mysql.createPool({
            host: 'host',
            user: 'user',
            password: 'password',
            database: 'database'
        });
        listen(ch, pool);
    });
});
