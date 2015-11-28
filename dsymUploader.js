var AdmZip = require('adm-zip');
var async = require('async');
var atosl = require('./atosl/atosl.js');
var express = require('express');
var fs = require('fs');
const hashstr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
var http = require('http');
var multer = require('multer');
var plist = require('plist')

var storage = multer.diskStorage({
    destination: function(req, file, cb) {
        cb(null, '/root/dsym')
    },
    filename: function(req, file, cb) {
        cb(null, hash() + '-' + Date.now() + '.zip')
    }
});

var upload = multer({
    storage: storage
})

var app = express();

function hash() {
    var hash = "";
    for (var i = 0; i < 64; i++) {
        hash += hashstr[Math.floor((Math.random() * 62))];
    }
    return hash;
}

app.post('/dsym', upload.single('dsym'), function(req, res, next) {
    var dsym = req.file;
    var baseDir = './' + dsym.filename.split('.')[0];
    var zip = new AdmZip('./' + dsym.filename);
    zip.extractAllTo(baseDir, true);
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
                for (a in arch) {
                    atosl.getUUID(arch[a], dPath);
                }
                var result = {
                    status: 200,
                    message: "success"
                };
                return callback(result);
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
    }, function(result) {
        if (!result) {
            result = {
                status: 400,
                message: "Cannot parse dSYM"
            };
        }
        res.status(result.status);
        res.json({
            message: result.message
        });
    });
});

http.createServer(app).listen(8080)
