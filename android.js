var amqp = require('amqplib/callback_api');
var mysql = require('mysql');

var QUEUE_NAME = 'oqa_android_log_queue';

function listen(ch, pool) {
    ch.assertQueue(QUEUE_NAME, {
        durable: true
    });
    ch.consume(QUEUE_NAME, function(msg) {
        pool.getConnection(function(err, connection) {
            if (!err) {
                var data = JSON.parse(msg.content.toString());
                connection.release();
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
