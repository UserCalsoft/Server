/** Module Holding logic to interact with Amazon S3 Service */

var config = require('./config.js');
var logger = require('./logger.js').logger;
var fs = require('fs');
var AWS = require('aws-sdk')
var client ;

exports.ImportPkgClient = function (Acc_key_Id,Secret_Key){
    //console.log('Acc_key_Id  '+Acc_key_Id+'----Secret_Key  '+Secret_Key)
    client = require('pkgcloud').storage.createClient({
        provider: 'amazon',
        keyId: Acc_key_Id, // access key id
        key: Secret_Key, // secret key
        region: config.Region,// region
        endpoint : new AWS.Endpoint(config.S3Endpoint)
    });
};

/*
 * Method to Get list of files in a bucket from S3
 * Input Params:
 *   containerName: Name of the bucket
 *   prefix: File Name prefix (Used for filtering purpose)
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.GetFiles = function (containerName,prefix, callback){
    client.getFiles(containerName,{prefix:prefix}, function (err, files) {
        callback(err,files);
    })
};

/*
 * Method to Get a file from the S3 bucket
 * Input Params:
 *   containerName: Name of the bucket
 *   fileName: Full Name of the file.
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.GetFile = function(containerName,fileName, callback){
    client.getFile(containerName, fileName, function (err, file) {
        callback(err,file);
    })
};

/*
 * Method to Upload a file on S3
 * Input Params:
 *   fileName:- full path of the file (excluding bucket name in it)
 *   container:- Name of the bucket
 * Output:
 *   WriteStream
 * */
exports.UploadFile = function(fileName, container) {
    var writeStream = client.upload({
        container: container,
        remote: fileName
    });

    writeStream.on('error', function (err) {
        logger.error('UploadFile:: File Upload Failed. Error:-'+err);
    });

    writeStream.on('success', function (file) {
        // success, file will be a File model
        logger.info('UploadFile:: File Upload Successful. File Name: %s', file.name);
    });

    //readStream.pipe(writeStream);
    return writeStream;
};

/*
 * Method to Download a file from S3
 * Input Params:
 *   fileName:- full path of the file (excluding bucket name in it)
 *   container:- Name of the bucket
 * Output:
 *   ReadStream
 * */
exports.DownloadFile = function(fileName,container){
    var readStream = client.download({
        container: container,
        remote: fileName
    });
    return readStream;
};

/*
 * Method to Delete a file From S3
 * Input Params:
 *   fileName:- full path of the file (excluding bucket name in it)
 *   container:- Name of the bucket
 *   callback: function to called on success/failure
 * Output:
 *   NA
 * */
exports.DeleteFile = function(fileName,container,callback){
    client.removeFile(container, fileName, function (err) {
        if(err){
            logger.error('DeleteFile:: File Delete Failed. Error:-'+err);
        }
        callback(err);
    })
};

/*
 * Method to Get the list of Buckets/Containers from S3
 * Input Params:
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.GetContainers = function(callback){
    client.getContainers(function (err, containers) {
        callback(err,containers);
    });
};

/*
 * Method to Get a specific bucket/container from S3
 * Input Params:
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.GetContainer = function (containerName, callback){
    client.getContainer(containerName, function (err, container) {
        callback(err,container);
    })
};

/*
 * Method to Create new Container/Bucket
 * Input Params:
 *   containerName : Name of the bucket/container
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.CreateContainer = function (containerName, callback){
    client.createContainer(containerName, function (err, container) {
        callback(err,container);
    });

};

/*
 * Method to Delete a Container/Bucket
 * Input Params:
 *   containerName : Name of the bucket/container
 *   callback: function to be called on success/failure
 * Output:
 *   NA
 * */
exports.DeleteContainer = function (containerName, callback){
    client.destroyContainer(containerName, function (err) {
        callback(err);
    });
};
