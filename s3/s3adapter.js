/** Module Holding logic to interact with Services implementing S3 Interface */

var config = require('../config.js');
var logger = require('../logger/logger.js').logger;
var AWS = require('aws-sdk');
var s3UploadStream = null;
var s3Client = null;

exports.Initialize = function (Access_Key_ID, Secret_Key) {
    logger.info('S3:Initialize: called with access key id:-'+ Access_Key_ID + ' and secret key:-*******');
    AWS.config.accessKeyId = Access_Key_ID;
    AWS.config.secretAccessKey = Secret_Key;
    var endPoint = new AWS.Endpoint(config.S3ServerEndPoint);
    s3Client = new AWS.S3({endpoint: endPoint});
    s3UploadStream = require('s3-upload-stream')(s3Client);
};

/***************************** Bucket Level API *************************************************************/
/**
 * Get List of buckets
 * */
exports.ListBuckets = function (callback) {
    logger.info('S3:ListBuckets: called for Listing Buckets');
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        s3Client.listBuckets(function (err, data) {
            if (err) {
                callback(err, null);
            }
            else {
                callback(null, data.Buckets);
            }
        });
    }
};

/**
 * Create new Bucket
 * Callback with Location on success
 * */
exports.CreateBucket = function (bucketName, callback) {
    logger.info('S3:CreateBucket: called for Create Bucket for Bucket Name- '+bucketName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            ACL: 'public-read-write' //'private | public-read | public-read-write | authenticated-read',
        };
        s3Client.createBucket(params, function (err, data) {
            callback(err, data);
        });
    }
};


/**
 * Delete Bucket
 * */
exports.DeleteBucket = function (bucketName, callback) {
    logger.info('S3:DeleteBucket: called for Delete Bucket for Bucket Name- '+bucketName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
        };
        s3Client.deleteBucket(params, function(err, data) {
            callback(err, data);
        });
    }
};

/**
 * Get Bucket Detail
 * API REF:- http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#headBucket-property
 * */
exports.GetBucketDetail = function (bucketName, callback) {
    logger.info('S3:GetBucketDetail: called for Get Bucket Detail for Bucket Name- '+bucketName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName /* Bucket Name (required) */
        };
        s3Client.listObjects(params, function(err,data){
            callback(err,data);
        });
    }
};

/***************************** Object API *************************************************************/
/**
 * List Objects in Bucket
 * API REF http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listObjects-property
 * */
exports.ListObjects = function (bucketName, prefix, callback) {
    logger.info('S3:ListObjects: called for listing object for Bucket Name- '+bucketName + 'with prefix '+ prefix);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Prefix : prefix,
            MaxKeys : config.FileListCountLimit
        };
        s3Client.listObjects(params, function(err, data) {
            callback(err, data);
        });
    }
};

/**
 * Upload Object in Bucket
 * return a writable stream
 * */
exports.UploadObject = function (bucketName, fileName) {
    logger.info('S3:UploadObject: called for upload object for Bucket Name- '+bucketName + 'with File '+ fileName);
    if (s3Client == null || s3UploadStream == null) {
        //callback(new Error("S3 Client not initialized"), null);
        return null;
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Key : fileName
        };
        var uploadStream = s3UploadStream.upload(params);

        // Optional configuration
        uploadStream.maxPartSize(20971520); // 20 MB
        uploadStream.concurrentParts(5);
        return uploadStream;
    }
};

/**
 * Create new Folder in Bucket
 * */
exports.CreateFolder = function (bucketName, folderPath, callback) {
    logger.info('S3:CreateFolder: called for create folder for Bucket Name- '+bucketName + 'with folder path '+ folderPath);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Key : folderPath,
            Body:"dir"
        };
        s3Client.upload(params,function(err,data){
            callback(err,data);
        });
    }
};

/**
 * Get Object (Content)
 * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#getObject-property
 * */
exports.GetObject = function (bucketName, fileName, callback) {
    logger.info('S3:GetObject: called for get object for Bucket Name- '+bucketName + 'with file name '+ fileName);
    if (s3Client == null) {
        //callback(new Error("S3 Client not initialized"), null);
        return null;
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Key : fileName
        };

        return s3Client.getObject(params).createReadStream();
    }
};

/**
 * Get Object Detail
 * API REF:- http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#headObject-property
 * */
exports.GetObjectDetail = function (bucketName, fileName, callback) {
    logger.info('S3:GetObjectDetail: called for get object details for Bucket Name- '+bucketName + 'with file name '+ fileName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Key : fileName
        };

        s3Client.headObject(params, function(err,data){
            callback(err,data);
        });
    }
};


/**
 * Delete Object
 * API ref: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteObject-property
 * */
exports.DeleteObject = function (bucketName, fileName, callback) {
    logger.info('S3:DeleteObject: called for delete object  for Bucket Name- '+bucketName + 'with file name '+ fileName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Key : fileName
        };

        s3Client.deleteObject(params, function(err,data){
            callback(err,data);
        });
    }
};

/**
 * Delete Multiple Objects
 * */
exports.deleteObjects = function (bucketName, arrfiles, callback) {
    logger.info('S3:deleteObjects: called for delete folder for Bucket Name- '+bucketName);
    if (s3Client == null) {
        callback(new Error("S3 Client not initialized"), null);
    }
    else {
        var params = {
            Bucket: bucketName, /* Bucket Name (required) */
            Delete : {
                Objects:[]
            }
        };
        for(var index = 0; index < arrfiles.length; index++){
            params.Delete.Objects.push({Key:arrfiles[index]});
        }

        s3Client.deleteObjects(params, function(err,data){
            callback(err,data);
        });
    }
};

/**
 * Copy Object
 * API REF:- http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#copyObject-property
 * */
exports.CopyObject = function (bucketName, copySource, destObjectKey, callback) {
    logger.info('S3:CopyObject: called for Bucket Name:- '+ bucketName + ', Source:-' + copySource + ', Destination:-' + destObjectKey);
    if (s3Client == null) {
        //callback(new Error("S3 Client not initialized"), null);
        return null;
    }
    else {
        var params = {
            Bucket: bucketName, /* required */
            CopySource: copySource, /* required */
            Key: destObjectKey, /* required */
        };
        s3Client.copyObject(params, function(err, data) {
            callback(err,data);
        });
    }
};


