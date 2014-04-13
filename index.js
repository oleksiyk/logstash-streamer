"use strict";

var path      = require('path');
var _         = require('lodash');
var fs        = require('fs');
var dgram   = require('dgram');

var LogStream = require('./lib/logstream')

var config = require(path.resolve(__dirname, './config.js'))
var udp = dgram.createSocket('udp4'),
    hostname = require('os').hostname(),
    logstashInd = 0;

fs.readdirSync(path.resolve(__dirname, './conf.d')).forEach(function(f) {
    f = path.resolve(__dirname, './conf.d/' + f)
    if(fs.statSync(f).isFile() && /\.js$/.test(f)){
        config.files = config.files.concat(require(f).files || [])
    }
})

var sendToLogstash = function (message, fields) {
    var data = {
        '@timestamp': new Date().toISOString(),
        host: hostname,
        message: message
    }

    _.merge(data, fields)

    var packet = JSON.stringify(data);
    packet = new Buffer(packet);
    udp.send(packet, 0, packet.length, config.logstash.port, config.logstash.hosts[logstashInd]);
    if(++logstashInd >= config.logstash.hosts.length){
        logstashInd = 0;
    }
}

config.files.forEach(function (file) {
    var log = new LogStream(file.path, {
        runtimeDir: config.runtimeDir,
        encoding: 'utf8',
        delay: file.delay || 1000
    })

    log.on('data', function(d) {
        var messages = []
        d.split('\n').forEach(function (message) {
            message = message.trim()
            if(!message){
                return
            }

            if(file.multiline && file.multiline.pattern && (file.multiline.pattern.test(message) === !!!file.multiline.negatePattern)){
                messages[messages.length-1] += '\n' + message
            } else {
                messages.push(message)
            }
        })

        messages.forEach(function (message) {
            sendToLogstash(message, file.fields || {})
        })
    });

    log.on('error', function (err) {
        console.error(err)
    })
})

// register exit handlers so that process.on('exit') works
var exitFunc = function(){
    console.log('Shutting down');
    process.exit(0);
}

process.on('SIGINT', exitFunc);
process.on('SIGTERM', exitFunc);


