var amqp = require('amqplib/callback_api');
var async = require('async');
var atosl = require('./atosl/atosl.js');
var crypto = require('crypto');
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
                if (j.hasOwnProperty('hqaData')) {
                    // iOS
                    async.waterfall([
                        function(callback) {
                            redisClient.hget('hqa_projects', j.apikey, function(err, pid) {
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
                            // Symbolication
                            // #1 : check buildid in Redis
                            // pass dSYM path (if exists)
                            j.buildid = j.buildid.toUpperCase(); // Atosl returns uppercase UUID, but client sends lowercase UUID
                            redisClient.hget('hqa_ios_dsym', pid + '_' + j.buildid, function(err, path) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, pid, path);
                                }
                            });
                        },
                        function(pid, path, callback) {
                            if (path != null) {
                                redisClient.hget('hqa_ios_arch', pid + '_' + j.buildid, function(err, arch) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        callback(null, pid, path, arch);
                                    }
                                });
                            } else {
                                callback(null, pid, path, null);
                            }
                        },
                        function(pid, path, arch, callback) {
                            // Symbolication
                            // #2 : Symbolication (if dSYM exists)
                            if (path != null && arch != null) {
                                var threadlog = '';
                                var crashedlog = ''; // for callstack key
                                for (var a = 0; a < j.hqaData.thread.length; a++) {
                                    var isCrashed = false;
                                    var thread = '';
                                    if (a != 0) {
                                        thread = '\n\n';
                                    }
                                    if (j.hqaData.thread[a].isCrashed == 1) {
                                        isCrashed = true;
                                        thread += 'Thread ' + a + ' (Crashed)';
                                    } else {
                                        thread += 'Thread ' + a;
                                    }
                                    for (var b = 0; b < j.hqaData.thread[a].frame.length; b++) {
                                        var result = '\n';
                                        var data = [j.hqaData.thread[a].frame[b].baseAddress, j.hqaData.thread[a].frame[b].frameIndex];
                                        if (j.hqaData.thread[a].frame[b].imageName == j.exename) {
                                            var symbolicated = atosl.symbolicate(arch, path, data);
                                            if (symbolicated == '' || symbolicated == null) {
                                                result += j.hqaData.thread[a].frame[b].imageName;
                                                result += ' ' + j.hqaData.thread[a].frame[b].frameIndex;
                                                result += '  ' + j.hqaData.thread[a].frame[b].baseAddress;
                                                result += ' + ' + j.hqaData.thread[a].frame[b].offset;
                                            } else {
                                                result += symbolicated;
                                            }
                                        } else {
                                            result += j.hqaData.thread[a].frame[b].imageName;
                                            result += ' ' + j.hqaData.thread[a].frame[b].frameIndex;
                                            result += '  ' + j.hqaData.thread[a].frame[b].baseAddress;
                                            result += ' + ' + j.hqaData.thread[a].frame[b].offset;
                                        }
                                        thread += result;
                                        if (isCrashed) {
                                            crashedlog += result;
                                            if (b == 3) {
                                                // hqaData line data
                                            }
                                        }
                                    }
                                    threadlog += thread;
                                }
                            } else {
                                // dSYM not exists
                                var threadlog = '';
                                var crashedlog = ''; // for callstack key
                                for (var a = 0; a < j.hqaData.thread.length; a++) {
                                    var isCrashed = false;
                                    var thread = '';
                                    if (a != 0) {
                                        thread = '\n\n';
                                    }
                                    if (j.hqaData.thread[a].isCrashed == 1) {
                                        isCrashed = true;
                                        thread += 'Thread ' + a + ' (Crashed)';
                                    } else {
                                        thread += 'Thread ' + a;
                                    }
                                    for (var b = 0; b < j.hqaData.thread[a].frame.length; b++) {
                                        var result = '\n';
                                        result += j.hqaData.thread[a].frame[b].imageName;
                                        result += ' ' + j.hqaData.thread[a].frame[b].frameIndex;
                                        result += '  ' + j.hqaData.thread[a].frame[b].baseAddress;
                                        result += ' + ' + j.hqaData.thread[a].frame[b].offset;
                                        thread += result;
                                        if (isCrashed == true) {
                                            crashedlog += result;
                                        }
                                    }
                                    threadlog += thread;
                                }
                            }
                            callback(null, pid, threadlog);
                        },
                        function(pid, callstack, callback) {
                            if (j.function.hasOwnProperty("symbol")) {
                                connection.query('SELECT * FROM errors where errorclassname=? and errorname=? and callstack=? and pid=?', [j.function.symbol.symbolName, j.errorname, j.errorreason, pid], function(err, rows) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        if (rows.length >= 1) {
                                            // UPDATE
                                            callback(null, pid, rows[0], callstack);
                                        } else {
                                            // INSERT
                                            callback(null, pid, null, callstack);
                                        }
                                    }
                                });
                            } else {
                                connection.query('SELECT * FROM errors where errorname=? and callstack=? and pid=?', [j.errorname, j.errorreason, pid], function(err, rows) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        if (rows.length >= 1) {
                                            // UPDATE
                                            callback(null, pid, rows[0], callstack);
                                        } else {
                                            // INSERT
                                            callback(null, pid, null, callstack);
                                        }
                                    }
                                });
                            }
                        },
                        function(pid, row, callstack, callback) {
                            if (row != null) {
                                connection.query('UPDATE errors SET numofinstances=?,wifion=?,mobileon=?,gpson=?,totalmemusage=?,lastdate=NOW() WHERE iderror=?', [row.numofinstances + 1, row.wifion + j.wifion, row.mobileon + j.mobileon, row.gpson + j.gpson, row.totalmemusage + j.appmemusage, row.iderror], function(err, result) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        callback(null, pid, row.iderror, callstack);
                                    }
                                });
                            } else {
                                if (j.function.hasOwnProperty("symbol")) {
                                    connection.query('INSERT INTO errors (callstack, rank, numofinstances, errorname, errorclassname, linenum, status, createdate, lastdate, wifion, mobileon, gpson, totalmemusage, pid) VALUES (?,?,?,?,?,?,?,NOW(),NOW(),?,?,?,?,?)', [j.errorreason, j.rank, 1, j.errorname, j.function.symbol.symbolName, '', 0, j.wifion, j.mobileon, j.gpson, j.appmemtotal, pid], function(err, result) {
                                        if (err) {
                                            callback(err, null);
                                        } else {
                                            callback(null, pid, result.insertId, callstack);
                                        }
                                    });
                                } else {
                                    connection.query('INSERT INTO errors (callstack, rank, numofinstances, errorname, errorclassname, linenum, status, createdate, lastdate, wifion, mobileon, gpson, totalmemusage, pid) VALUES (?,?,?,?,?,?,?,NOW(),NOW(),?,?,?,?,?)', [j.errorreason, j.rank, 1, j.errorname, j.function, '', 0, j.wifion, j.mobileon, j.gpson, j.appmemtotal, pid], function(err, result) {
                                        if (err) {
                                            callback(err, null);
                                        } else {
                                            callback(null, pid, result.insertId, callstack);
                                        }
                                    });
                                }
                            }
                        },
                        function(pid, errorid, callstack, callback) {
                            connection.query('INSERT INTO instances (sdkversion, locale, datetime, device, country, appversion, osversion, gpson, wifion, mobileon, scrwidth, scrheight, batterylevel, availsdcard, rooted, appmemtotal, appmemfree, appmemmax, kernelversion, xdpi, ydpi, scrorientation, sysmemlow, lastactivity, iderror, pid) VALUES (?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [j.sdkversion, j.locale, j.device, j.country, j.appversion, j.osversion, j.gpson, j.wifion, j.mobileon, j.scrwidth, j.scrheight, j.batterylevel, j.availsdcard, j.Jailbreak, j.appmemtotal, j.appmemfree, j.appmemtotal, j.kernelversion, null, null, j.scrorientation, j.sysmemlow, '', errorid, pid], function(err, result) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, result.insertId, callstack);
                                }
                            });
                        },
                        function(instanceid, callstack, callback) {
                            connection.query('INSERT INTO instancelog (idinstance, log, savetime) VALUES (?,?,NOW())', [instanceid, callstack], function(err, result) {
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
                } else {
                    // Android
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
                            connection.query('SELECT * FROM errors where callstack=? and pid=?', [j.exception.callstack, pid], function(err, rows) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    if (rows.length >= 1) {
                                        // UPDATE
                                        callback(null, pid, rows[0]);
                                    } else {
                                        // INSERT
                                        callback(null, pid, null);
                                    }
                                }
                            });
                        },
                        function(pid, row, callback) {
                            if (row != null) {
                                connection.query('UPDATE errors SET numofinstances=?,wifion=?,mobileon=?,gpson=?,totalmemusage=?,lastdate=NOW() WHERE iderror=?', [row.numofinstances + 1, row.wifion + j.exception.wifion, row.mobileon + j.exception.mobileon, row.gpson + j.exception.gpson, row.totalmemusage + j.exception.appmemtotal, row.iderror], function(err, result) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        callback(null, pid, row.iderror);
                                    }
                                });
                            } else {
                                connection.query('INSERT INTO errors (callstack, rank, numofinstances, errorname, errorclassname, linenum, status, createdate, lastdate, wifion, mobileon, gpson, totalmemusage, pid) VALUES (?,?,?,?,?,?,?,NOW(),NOW(),?,?,?,?,?)', [j.exception.callstack, j.exception.rank, 1, j.exception.errorname, j.exception.errorclassname, j.exception.linenum, 0, j.exception.wifion, j.exception.mobileon, j.exception.gpson, j.exception.appmemtotal, pid], function(err, result) {
                                    if (err) {
                                        callback(err, null);
                                    } else {
                                        callback(null, pid, result.insertId);
                                    }
                                });
                            }
                        },
                        function(pid, errorid, callback) {
                            connection.query('INSERT INTO instances (sdkversion, locale, datetime, device, country, appversion, osversion, gpson, wifion, mobileon, scrwidth, scrheight, batterylevel, availsdcard, rooted, appmemtotal, appmemfree, appmemmax, kernelversion, xdpi, ydpi, scrorientation, sysmemlow, lastactivity, iderror, pid) VALUES (?,?,NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [j.exception.sdkversion, j.exception.locale, j.exception.device, j.exception.country, j.exception.appversion, j.exception.osversion, j.exception.gpson, j.exception.wifion, j.exception.mobileon, j.exception.scrwidth, j.exception.scrheight, j.exception.batterylevel, j.exception.availsdcard, j.exception.rooted, j.exception.appmemtotal, j.exception.appmemfree, j.exception.appmemmax, j.exception.kernelversion, j.exception.xdpi, j.exception.ydpi, j.exception.scrorientation, j.exception.sysmemlow, j.exception.lastactivity, errorid, pid], function(err, result) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, pid, errorid, result.insertId);
                                }
                            });
                        },
                        function(pid, errorid, iid, callback) {
                            var i = 1;
                            async.eachSeries(j.exception.eventpaths, function iterator(item, callback) {
                                connection.query('INSERT INTO eventpaths (idinstance, iderror, ins_count, datetime, classname, methodname, linenum, depth, label) VALUES (?,?,?,NOW(),?,?,?,?,?)', [iid, errorid, null, item.classname, item.methodname, item.linenum, i++, item.label], function(err, result) {
                                    if (err) {
                                        callback(err);
                                    } else {
                                        callback();
                                    }
                                });
                            }, function(err) {
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
