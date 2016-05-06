/**
 * Module to have ftp server logic
 * Also interfacing with S3
 * */

var config = require('./config.js');
var s3cloud = require('./pkgcloud.js');
var AWS = require('aws-sdk');
var ftpd = require('ftpd');
var fs = require('fs');
var path = require('path');
var logger = require('./logger.js').logger;
var server;
var options = {
    host: config.ServerIP || '127.0.0.1',
    port: config.ServerPort || 7002,
    tls: null
};

var pathSeperator = path.sep;


var getFileState = function (size, lastModified, callback) {
    fs.stat(process.cwd() + '/bucketwithfile/1.txt', function (err, stats) {
        stats.size = size;
        stats.mtime = lastModified;
        logger.info('stat:getFileState:: File state information returned. File Size %d', stats.size);
        callback(err, stats);
    });
};

var getFolderState = function (size, lastModified,callback) {
    fs.stat(process.cwd() + '/bucketwithfile', function (err, stats) {
        logger.info('stat:getFolderState:: Folder state information (empty folder) returned');
        stats.size = size;
        stats.mtime = lastModified;
        callback(err, stats);
    });
};

var getContainerState = function (container, callback) {
    if (container && container.files.length > 0) {
        fs.stat(process.cwd() + '/bucketwithfile', function (err, stats) {
            logger.info('stat:getContainerState:: Container state information (non-empty folder) returned');
            callback(err, stats);
        });
    }
    else {
        fs.stat(process.cwd() + '/bucketwithfile', function (err, stats) {
            logger.info('stat:getContainerState:: Container state information (empty folder) returned');
            callback(err, stats);
        });
    }
};
/*
 * Object containing Custom FS implementation for FTP Server actions
 * */
var filecount=0;
var fsImplementation = {
    /*
    * */
    readdir: function (path, callback) {
        logger.info('readdir:: called for path ' + path);
        if (path == pathSeperator) {
            //For root level access return list of buckets;
            logger.info('readdir:: Found root path hence getting containers(buckets) from S3');
            s3cloud.GetContainers(function (err, containers) {
                if(err){
                    logger.info('readdir:: Failed to Get Containers from S3. Error: ' + err);
                    callback(err,null);
                    return;
                }
                logger.info('readdir:: Containers returned by S3 is ' + containers.length);
                var arr_container = [];
                for (var i = 0; i < containers.length; i++) {
                    arr_container.push(containers[i].name);
                }
                logger.info('readdir:: Calling back with container list:' + JSON.stringify(arr_container));
                callback(null, arr_container);
            });
        }
        else {
            //Read files from one of the bucket
            var tmpPath = path.substr(1);
            var pathArray = tmpPath.split(pathSeperator);
            var containerName = pathArray[0];
            var prefix = '';
            if (pathArray.length > 1) {
                for (var cnt = 1; cnt < pathArray.length; cnt++) {
                    prefix += pathArray[cnt] + '/';
                }
            }
            logger.info('readdir:: Found non-root path hence getting files from Container %s, and Prefix %s for Path %s', containerName, prefix, path);
            s3cloud.GetFiles(containerName, prefix, function (err, files) {
                if (err) {
                    logger.error('readdir:: Error occured in getting files. Err' + err);
                    return
                }
                logger.info('readdir:: Number of Files returned from S3 %d', files.length);
                var arr_files = [];
                for (var i = 0; i < files.length; i++) {
                    var fileName = files[i].name.substr(prefix.length); //Remove the prefix from the name
                    var fileNameParts = fileName.split('/');
                    if (fileNameParts.length > 1) {
                        //It is an directory
                        var parentDir = fileNameParts[0] + '/';
                        if (arr_files.indexOf(parentDir) < 0) {
                            arr_files.push(parentDir)
                        }
                    }
                    else {
                        if(fileNameParts[0] !=""){
                            arr_files.push(fileNameParts[0]);
                        }
                    }
                }
                var limit = config.FileListCountLimit;
                if(arr_files.length > config.FileListCountLimit){
                    limit = config.FileListCountLimit;
                }
                else{
                    limit = arr_files.length
                }
                sub_arr_files = arr_files.slice(0,limit);
                logger.info('readdir:: Calling back with file list' + JSON.stringify(sub_arr_files));
                filecount = sub_arr_files.length;
                callback(null,sub_arr_files );
            });
        }
    },
    stat: function (file, callback) {


        logger.info('stat:: Called for Path %s', file);
        var tmpFile = file.substr(1);
        var fileArray = tmpFile.split(pathSeperator);
        var containerName = fileArray[0];

        if (fileArray.length == 1) {
            logger.info('stat:: Getting container %s from S3', containerName);
            s3cloud.GetContainer(containerName, function (err, container) {
                if (err) {
                    logger.error('stat:: Error occurred in getting container from S3. Error - ' + err);
                    //TODO: Not sure if we should still list such containers;
                    getFolderState(callback);
                    //callback(err,null);
                    return;
                }
                logger.info('stat:: Container information retrieved from S3, Getting stats for container');
                getContainerState(container, callback);
            });
        }
        else {
            if(filecount> 0){
                filecount -= 1;
            }else{
                file += pathSeperator;
                fileArray = file.substr(1).split(pathSeperator);
            }

            var isDirectory = false;
            var fileName = '';
            if (fileArray.length > 1) {
                for (var cnt = 1; cnt < fileArray.length; cnt++) {
                    if (fileArray[cnt] == '' && cnt == fileArray.length - 1) {
                        isDirectory = true;
                    }
                    else {
                        fileName += fileArray[cnt];
                        if (cnt != fileArray.length - 1) {
                            fileName += '/';
                        }
                    }
                }
            }
                logger.info('stat:: Getting File from S3.Container Name- %s, File Name- %s', containerName, fileName);
                s3cloud.GetFile(containerName, fileName, function (err, file) {
                    if (err) {
                        logger.error('stat:: Error occurred while getting file from S3. Error-' + err);
                        callback(err, null);
                        return;
                    }
                    if(isDirectory){
                        getFolderState(file.size,file.lastModified, callback);
                    }else{
                        getFileState(file.size, file.lastModified, callback);
                    }

                });

        }
    },
    open: function (fileName, mode, callback) {
        var opt = {fn: fileName};
        callback(null, opt);
    },
    unlink: function (file, callback) {
        var tmpFile = file.substr(1);
        var filePathParts = tmpFile.split(pathSeperator);
        var containerName = filePathParts[0];
        var fileName = '';
        for (var i = 1; i < filePathParts.length; i++) {
            fileName += filePathParts[i];
            if (i != filePathParts.length - 1) {
                fileName += '/';
            }
        }
        s3cloud.DeleteFile(fileName, containerName, callback);
    },
    mkdir: function (path, mode, callback) {
        var tmpPath = path.substr(1);
        var arr_path = tmpPath.split(pathSeperator);
        var containerName = arr_path[0];

        if(arr_path.length == 1){
            //Its a call to create new bucket
            s3cloud.CreateContainer(containerName,function(err,container){
                if(err){
                    logger.error('mkdir:: Failed to create new container/bucket on S3. Error-' + err);
                    callback(err,null);
                    return;
                }
                logger.info('mkdir:: New container/bucket created successfully on S3.');
                callback(err,container);
            })
        }
        else{
            var folderPath = "";
            for(var i=1;i<arr_path.length;i++){
                folderPath += arr_path[i] + '/';
            }
            s3cloud.UploadFile(folderPath,containerName);
        }

    },

    createWriteStream: function (file, options) {
        var tmpFile = file.substr(1);
        var filePathParts = tmpFile.split(pathSeperator);
        var containerName = filePathParts[0];
        var fileName = '';
        for (var i = 1; i < filePathParts.length; i++) {
            fileName += filePathParts[i];
            if (i != filePathParts.length - 1) {
                fileName += '/';
            }
        }
        return s3cloud.UploadFile(fileName, containerName);
    },
    createReadStream: function (unknown, options) {
        var file = options.fd.fn;
        var tmpFile = file.substr(1);
        var filePathParts = tmpFile.split(pathSeperator);
        var containerName = filePathParts[0];
        var fileName = '';
        for (var i = 1; i < filePathParts.length; i++) {
            fileName += filePathParts[i];
            if (i != filePathParts.length - 1) {
                fileName += '/';
            }
        }
        return s3cloud.DownloadFile(fileName, containerName);
    },

};

exports.Start = function () {
    server = new ftpd.FtpServer(options.host, {
        getInitialCwd: function () {
            return '';
        },
        getRoot: function () {
            return '';
        },
        pasvPortRangeStart: 1025,
        pasvPortRangeEnd: 1050,
        tlsOptions: options.tls,
        allowUnauthorizedTls: true,
        useWriteFile: false,
        useReadFile: false,
        uploadMaxSlurpSize: 7000, // N/A unless 'useWriteFile' is true.

    });
    server.on('error', function (error) {
        logger.error('Start:: Error Starting FTP Server. ' + error);
    });

    server.on('client:connected', function (connection) {
        var username = null;
        console.log('client connected: ' + connection.remoteAddress);
        connection.on('command:user', function (user, success, failure) {
            if (user) {
                username = user;
                success();
            } else {
                failure();
            }
        });


        connection.on('command:pass', function (pass, success, failure) {
            //TODO:- Put authentication logic here
            if (pass) {
                success(username, fsImplementation);
            } else {
                failure();
            }
        });
    });

    server.debugging = 0;
    server.listen(options.port);
    logger.info('Igneous FTP Server Listening on port  ' + options.port);
}
