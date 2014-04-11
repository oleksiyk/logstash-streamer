"use strict";

var path = require('path');
var fs = require('fs');
var util = require("util");
var stream = require("stream");
var crypto = require('crypto');
var path = require('path');
var Promise = require('bluebird')
var persistent = require('fs-persistent-object');

var LogStream = function(filename, options){

    this.options = options;

    stream.Readable.call(this, options);

    this.fileInfo = persistent.load(path.resolve(options.runtimeDir, crypto.createHash('md5').update(filename).digest("hex")), {
        filename: filename,
        offset: 0,
        hash: null,
        hashPos: 512
    });

    if(/\.1$/.test(filename)){ // /var/log/proftpd/xferlog.1
        this.isRotatedFile = true;
    }

    this.readBuffer = new Buffer(16384);
}

util.inherits(LogStream, stream.Readable);

module.exports = LogStream;

LogStream.prototype.readFileChunk = function(size){
    var self = this;

    return new Promise(function (resolve, reject) {
        if(self.readBuffer.length < size){
            self.readBuffer = new Buffer(size);
        }

        fs.read(self.fd, self.readBuffer, 0, size, self.fileInfo.offset, function(err, bytesRead){
            if(err){
                return reject(err);
            }

            while(bytesRead > 0 && self.readBuffer[bytesRead-1] !== 10){ // \n
                bytesRead--;
            }

            self.fileInfo.offset += bytesRead;

            resolve(self.readBuffer.slice(0, bytesRead));
        })
    })
}


LogStream.prototype.verifyFileInfo = function(){
    var self = this;

    return new Promise(function (resolve, reject) {

        fs.read(self.fd, self.readBuffer, 0, 512, 0, function(err, bytesRead){
            if(err){
                return reject(err);
            }

            if(bytesRead > 0){
                var pos = self.fileInfo.hashPos;

                if(bytesRead < pos){
                    pos = bytesRead;
                }

                var hash = crypto.createHash('md5').update(self.readBuffer.slice(0, pos)).digest("hex");

                if(self.fileInfo.hash != hash){ // content has changed, so that is a new file
                    if(self.isRotatedFile){
                        return reject(new Error(self.fileInfo.filename + ' is not a recently rotated file we expected, not parsing it!'));
                    }
                    if(self.fileInfo.hash && !self.isRotatedFile){
                        self.createRotatedStream();
                    }
                    self.fileInfo.offset = 0;
                }

                self.fileInfo.hash = crypto.createHash('md5').update(self.readBuffer.slice(0, bytesRead)).digest("hex");
                self.fileInfo.hashPos = bytesRead;
            }

            resolve()
        })
    })
}

LogStream.prototype.createRotatedStream = function(){
    var self = this;
    var rf = self.fileInfo.filename + '.1';
    var hash = self.fileInfo.hash;
    var offset = self.fileInfo.offset;
    var hashPos = self.fileInfo.hashPos;

    fs.exists(rf, function(exists){
        if(exists){
            console.log('Checking rotated file ' + rf);

            self.rotatedStream = new LogStream(rf, self.options);
            self.rotatedStream.fileInfo.hash = hash;
            self.rotatedStream.fileInfo.hashPos = hashPos;
            self.rotatedStream.fileInfo.offset = offset;

            self.rotatedStream.on('end', function(){
                self.rotatedStream = null;
            })

            self.rotatedStream.on('data', function(data){
                console.log('Got some data from rotated file ' + rf);
                self.push(data);
            })
        }
    });
}

LogStream.prototype._open = function () {
    var self = this;
    return new Promise(function (resolve, reject) {
        fs.open(self.fileInfo.filename, 'r', function(err, fd){
            if(err){
                return reject(err);
            }
            self.fd = fd;
            resolve()
        });
    })
}

LogStream.prototype._read = function(size){

    var self = this;

    self._open()
    .then(function () {
        return self.verifyFileInfo()
    })
    .then(function () {
        return self.readFileChunk(size)
    })
    .catch(function (err) {
        if(err.code !== 'ENOENT'){
            self.emit('error', err)
            throw err
        }
    })
    .then(function (buf) {
        buf = buf || '';

        if(buf.length === 0){
            if(self.isRotatedFile){
                buf = null; // signal end of data because no one will append to rotated file
            } else {
                setTimeout(function () {
                    self.read(0)
                }, self.options.delay || 1000)
            }
        }

        if(self.fd){
            fs.close(self.fd)
        }

        self.fd = null;

        // push data to stream
        self.push(buf);
    })

}
