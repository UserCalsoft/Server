/**
 * Logging Module
 */

var winston = require('winston');

var logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            level:'info'
        }),
        new (winston.transports.File)({
            level: 'error',
            filename: 'igneous-ftp-server.log'
        })
    ]
});

exports.logger = logger;
