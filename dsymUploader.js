var AdmZip = require('adm-zip');
var async = require('async');
var atosl = require('./atosl/atosl.js');
var bodyParser = require('body-parser');
var express = require('express');
var fs = require('fs');
var fse = require('fs-extra');
const hashstr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
var http = require('http');
var multer = require('multer');
var mysql = require('mysql');
var plist = require('plist')
var redis = require('redis'),
    redisClient = redis.createClient('redis://host:port');

var upload = multer({
    storage: multer.diskStorage({
        destination: function(req, file, cb) {
            cb(null, '/root/dsymzip')
        },
        filename: function(req, file, cb) {
            cb(null, hash() + '-' + Date.now() + '.zip')
        }
    })
})

var app = express();
app.use(bodyParser.urlencoded({
    extended: true
}));

var pool = mysql.createPool({
    host: 'host',
    user: 'user',
    password: 'password',
    database: 'database'
});

function hash() {
    var hash = "";
    for (var i = 0; i < 64; i++) {
        hash += hashstr[Math.floor((Math.random() * 62))];
    }
    return hash;
}

app.post('/dsym', upload.single('dsym'), function(req, res, next) {
    redisClient.hget('hqa_projects', req.body.apikey, function(err, pid) {
        if (err) {
            res.status(500);
            res.json({
                message: "Redis error"
            });
        } else {
            if (pid == null) {
                res.status(400);
                res.json({
                    message: "API Key not exists"
                });
            } else {
                var dsym = req.file;
                var baseDir = '/root/dsymzip/' + dsym.filename.split('.')[0];
                var zip = new AdmZip('/root/dsymzip/' + dsym.filename);
                zip.extractAllTo(baseDir, true);
                fse.removeSync('/root/dsymzip/' + dsym.filename);
                var folder = fs.readdirSync(baseDir + '/', 'utf8');
                async.eachSeries(folder, function iterator(f, callback) {
                        if (f.indexOf(".app.dSYM") != -1) {
                            var raw_info = fs.readFileSync(baseDir + '/' + f + '/Contents/Info.plist', 'utf8');
                            var info = plist.parse(raw_info);
                            var id = info["CFBundleIdentifier"].split('.');
                            var version = info["CFBundleShortVersionString"];
                            var dPath = baseDir + '/' + f + '/Contents/Resources/DWARF/' + id[id.length - 1];
                            var arch = atosl.getArch(dPath);
                            if (arch != null) {
                                pool.getConnection(function(err, connection) {
                                    if (!err) {
                                        fse.copySync(dPath, '/root/dsym/' + dsym.filename.split('.')[0])
                                        async.eachSeries(arch, function iterator(a, insertCallback) {
                                                var uuid = atosl.getUUID(a, dPath);
                                                if (uuid != null) {
                                                    connection.query('INSERT into dsym (architecture,uuid,filename,uploadtime,pid) VALUES (?,?,?,NOW(),?)', [a, uuid, '/root/dsym/' + dsym.filename.split('.')[0], pid], function(err, rows) {
                                                        if (err) {
                                                            insertCallback(err);
                                                        } else {
                                                            insertCallback();
                                                        }
                                                    });
                                                }
                                            },
                                            function(err) {
                                                if (err) {
                                                    return callback(null);
                                                } else {
                                                    var result = {
                                                        status: 200,
                                                        message: "success"
                                                    };
                                                    return callback(result);
                                                }
                                            });
                                    } else {
                                        var result = {
                                            status: 500,
                                            message: "cannot connect to db"
                                        };
                                        return callback(result);
                                    }
                                });
                            } else {
                                var result = {
                                    status: 400,
                                    message: "Cannot parse data from dSYM"
                                };
                                return callback(result);
                            }
                        } else {
                            callback();
                        }
                    },
                    function(result) {
                        if (result == null) {
                            result = {
                                status: 400,
                                message: "Cannot parse dSYM"
                            };
                        }
                        fse.removeSync(baseDir);
                        res.status(result.status);
                        res.json({
                            message: result.message
                        });
                    });
            }
        }
    });
});

http.createServer(app).listen(8080)
