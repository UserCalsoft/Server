/**
 * Module to have ftp server logic
 * Also interfacing with S3
 * */

var config = require('../config.js');
var logger = require('../logger/logger.js').logger;
var ftpd = require('ftpd');
var fs = require('fs');
var path = require('path');
var server;
var pathSeparator = path.sep;
var s3PathSeparator ='/';

/******************************** [START] Custom Implementation for FS Operations ****************************************/
/**
 * Utility Method to Parse FTP Path string
 * return object containing various useful information from the path.
 * */
var parseFtpPath = function(path){
    logger.info('ftp:parseFtpPath:Called For Path - '+path);
    var pathObject = {
        isRootPath : false,
        bucketName : null,
        objectKey : null,
        isFolderObject : false
    };
    if(path == pathSeparator){
        logger.info('ftp:parseFtpPath:Called For rootpath - '+path);
        pathObject.isRootPath = true;
    }
    else{
        logger.info('ftp:parseFtpPath:Called For Folder path - '+path);
        var pathFragments = path.split(pathSeparator);
        pathObject.bucketName = pathFragments[1];
        //Prepare S3 Object Key
        if(pathFragments.length > 2){
            pathFragments.splice(0,2);
            pathObject.objectKey = pathFragments.join(s3PathSeparator);
        }
        //Check if the path is for folder
        if(this.endsWithUtil(path,pathSeparator)){
            pathObject.isFolderObject = true;
        }
        //else if(this.connection.cwd == path || this.statCounter == 0){
        //    pathObject.isFolderObject = true;
        //}
        //Add the trailing "/" if object is of folder type
        if(pathObject.isFolderObject && pathObject.objectKey != null && this.endsWithUtil(pathObject.objectKey, s3PathSeparator) == false){
            pathObject.objectKey += s3PathSeparator;
        }
    }
    return pathObject;
};

/**
 * Utility Method to Create Stat object for File/Folders
 * return object containing Stat information.
 * */
var getStats = function(mode, isDirectory,size,mTime){
    logger.info('ftp:getStats:Called For get Stats');
    var stat ={
        mode:mode || 12345,
        size:size||0,
        mtime:mTime
    };
    if(isDirectory){
        logger.info('ftp:getStats:Checking for isDirectory, which return true');
        stat.isDirectory = function(){
            return true;
        }
    }
    else{
        logger.info('ftp:getStats:Checking for isDirectory, which return false');
        stat.isDirectory = function(){
            return false;
        }
    }
    return stat;
};

/**
 * Utility Method to chekc if given string ends with the given suffix
 * return boolean.
 * */
var endsWith = function(str, suffix) {
    return str.indexOf(suffix, str.length - suffix.length) !== -1;
};

/**
 * This method will be invoked on every list command
 * */
var readDir = function(path,callback){
    logger.info('ftp:readDir:Reading Dir For path - '+path);
    var pathObject = this.parseFtpPathUtil(path);
    var self = this;
    if(pathObject.isRootPath){
        //We are on root path so we have to list all the buckets as directories on root
        this.s3adapter.ListBuckets(function(err,buckets){
            if(err){
                logger.error('ftp:readDir:list: Error occurred in Listing Buckets. Err- '+err);
                self.localStore.buckets = [];
                callback(err,null);
            }
            else{
                logger.info('ftp:readDir: Listing Buckets. Bucket count:-' + buckets.length);
                var bucketList =[];
                self.localStore.buckets = [];
                buckets.forEach(function(bucket){
                    self.localStore.buckets.push(bucket);
                    bucketList.push(bucket.Name);
                });
                callback(null,bucketList);
            }
        });
    }
    else{
        //We are inside one of the bucket, so we have to list all the objects in the bucket.
        var prefix = "";
        //Add trailing "/" in prefix as prefix will always be a folder
        if(pathObject.objectKey){
            prefix = pathObject.objectKey + s3PathSeparator;
        }

        this.s3adapter.ListObjects(pathObject.bucketName, prefix, function(err,data){
            if(err){
                logger.error('ftp:readDir:listinbucket: Error occurred in Listing Folders. Err- '+err);
                self.localStore.objects = [];
                callback(err,null);
            }
            else{
                logger.info('ftp:readDir: Listing Folders. Object count: ' + data.Contents.length);
                self.localStore.objects = [];
                var objectList = [];
                data.Contents.forEach(function(obj){
                    self.localStore.objects.push(obj);
                    var objNameParts = obj.Key.substr(prefix.length).split(s3PathSeparator);
                    if(objNameParts.length > 1){
                        //Object is part of some directory, hence pushing it into the list (if not already present
                        if (objectList.indexOf(objNameParts[0]+s3PathSeparator) < 0) {
                            objectList.push(objNameParts[0]+s3PathSeparator);
                        }
                    }
                    else if(objNameParts[0] != ""){
                        objectList.push(objNameParts[0]);
                    }
                });
                self.statCounter = objectList.length;
                callback(null,objectList);
            }
        });
    }
};

/**
 * This method will be invoked for each folder/file before returning the listing to the ftp client
 * */
var stat = function(path,callback){
    logger.info('ftp:stat: Called for Path %s', path);
    var pathObject = this.parseFtpPathUtil(path);
    var self = this;
    var stats = null;
    if(pathObject.isRootPath){
        callback(null,this.getStatsUtil(null,true,0,null));
    }
    else if(pathObject.bucketName != null && pathObject.objectKey == null){
        //Get Bucket Details from local store
        for(var bucketIndex = 0; bucketIndex < this.localStore.buckets.length; bucketIndex++){
            if(this.localStore.buckets[bucketIndex].Name == pathObject.bucketName){
                stats = this.getStatsUtil(null,true,0,this.localStore.buckets[bucketIndex].CreationDate);
                break;
            }
        }
        if(stats == null){
            //If User change directory before ls command then local Store will not have bucket list in cache.
            // Hence get if from S3 and put it in local store for subsequent use.
            this.s3adapter.ListBuckets(function(err,buckets){
                if(err){
                    logger.error('ftp:stat:list: Error occurred in Listing Bucket. Err- '+err);
                    self.localStore.buckets = [];
                    callback(err,null);
                }
                else{
                    var bucketList =[];
                    self.localStore.buckets = [];
                    buckets.forEach(function(bucket){
                        self.localStore.buckets.push(bucket);
                        if(bucket.Name == pathObject.bucketName){
                            stats = self.getStatsUtil(null,true,0,self.localStore.buckets[bucketIndex].CreationDate);
                        }
                        bucketList.push(bucket.Name);
                    });
                    if(stats !== null){
                        callback(null,stats);
                    }
                    else{
                        logger.error('ftp:stat:list: Bucket details not found');
                        callback(new Error("Bucket details not found"),null);
                    }
                }
            });
        }
        else{
            callback(null,stats);
        }
    }
    else if(pathObject.bucketName != null && pathObject.objectKey != null){
        //Add trailing "/" if statCounter is 0. Handle case of direct call of stat without call of readdir.
        if(this.statCounter == 0 && this.endsWithUtil(pathObject.objectKey, s3PathSeparator) == false){
            pathObject.objectKey += s3PathSeparator;
            pathObject.isFolderObject = true;
        }

        for(var objIndex =0; objIndex < self.localStore.objects.length; objIndex++){
            if(self.localStore.objects[objIndex].Key == pathObject.objectKey){
                stats = self.getStatsUtil(null,pathObject.isFolderObject,self.localStore.objects[objIndex].Size,self.localStore.objects[objIndex].LastModified);
                break;
            }
        }
        if(stats != null){
            callback(null,stats);
        }
        else{
            logger.info('ftp:stat:froms3: Since State could not found in local storage hence try to find it from S3');
            //Since State could not found in local storage hence try to find it from S3
            this.s3adapter.GetObjectDetail(pathObject.bucketName, pathObject.objectKey,function(err,data){
                //Get Object Details from S3 Server
                if(err){
                    logger.error('ftp:stat: Error occurred while  getting object detail from S3. Err- '+err);
                    callback(err,null);
                }
                else{
                    var stats = self.getStatsUtil(null,pathObject.isFolderObject,data.ContentLength,data.LastModified);
                    callback(null,stats);
                }
            });
        }
    }
    else{
        //TODO: Log exception as this should never happen
        logger.error('ftp:stat: Failed to get stat as Bucket and ObjectKey both are null');
        callback(new Error("Failed to get stat as Bucket and ObjectKey both are null"),null);
    }

    //Decrement the stat counter after processing, if it is greater than 0
    if(this.statCounter > 0)
        this.statCounter -= 1;
};

/**
 * This method will be invoked when file is deleted
 * */
var unLink = function(fileName,callback){
    var pathObject = this.parseFtpPathUtil(fileName);
    var self = this;
    if(pathObject.bucketName != null && pathObject.objectKey == null){
        //The Command is to Delete bucket
        this.s3adapter.DeleteBucket(pathObject.bucketName,function(err,data){
            if(err){
                logger.error('ftp:unLink: Error occurred while deleting Bucket. Err- '+err);
            }
            callback(err);
        });
    }
    else if(pathObject.bucketName != null && pathObject.objectKey != null) {
        self.s3adapter.GetObjectDetail(pathObject.bucketName, pathObject.objectKey, function (err, data) {
            //Get object with the give object key
            if (!err) {
                //Object found, hence delete it
                self.s3adapter.DeleteObject(pathObject.bucketName, pathObject.objectKey, function (err, data) {
                    if (err) {
                        logger.error('ftp:unLink: Error occurred while deleting Object. Err- '+err);
                    }
                    callback(err)
                });
            }
            else {
                //Unlink usually called for files so this code block should never hit.
                //It is just for fall back
                //Object key not found. It can be a directory so find it again with trailing "/"
                pathObject.objectKey += s3PathSeparator;
                self.s3adapter.GetObjectDetail(pathObject.bucketName, pathObject.objectKey, function (err, data) {
                    if (!err) {
                        //Get all other files in this folder
                        self.s3adapter.ListObjects(pathObject.bucketName, pathObject.objectKey, function (err, data) {
                            if (!err) {
                                var objectsToDelete = [];
                                objectsToDelete.push(pathObject.objectKey);
                                data.Contents.forEach(function (object) {
                                    objectsToDelete.push(object.Key);
                                });
                                self.s3adapter.deleteObjects(pathObject.bucketName, objectsToDelete, function (err, data) {
                                    if (err) {
                                        logger.error('fpt:unLink: Error occurred while deleting folder. Err- '+err);
                                    }
                                    callback(err);
                                });
                            }
                            else {
                                logger.error('ftp:unLink: Error occurred while listing  folders to delete. Err- '+err);
                                callback(err);
                            }
                        })
                    }
                    else {
                        logger.error('ftp:unLink: Error occurred while  getting  folders details to delete. Err- '+err);
                        callback(err);
                    }
                });
            }
        });
    }
    else{
        logger.error('ftp:unLink: Bucket Name and Object Key both are null');
        callback(new Error("Bucket Name and Object Key both are null"));
    }
};

/**
 * This method will be invoked when new directory is created
 * */
var mkDir = function(path,access,callback){
    var pathObject = this.parseFtpPathUtil(path);
    if(pathObject.bucketName != null && pathObject.objectKey == null){
        //The Command is to create new bucket
        this.s3adapter.CreateBucket(pathObject.bucketName,function(err,data){
            if(err){
                logger.error('ftp:mkDir: Error occurred while  creating Bucket. Err- '+err);
            }
            callback(err);
        });
    }
    else if(pathObject.bucketName != null && pathObject.objectKey != null){
        //The command is to create new folder object under the bucket
        //Append trailing "/" in the object key (if not already exists) to create folder in S3
        if(this.endsWithUtil(pathObject.objectKey,s3PathSeparator) == false){
            pathObject.objectKey += s3PathSeparator;
        }
        this.s3adapter.CreateFolder(pathObject.bucketName,pathObject.objectKey, function(err,data){
            if(err){
                logger.error('ftp:mkDir: Error occurred while  creating folder. Err- '+err);
            }
            callback(err);
        });

    }
    else{
        logger.error('ftp:mkDir: Bucket Name and Object Key both are null');
        callback(new Error("Bucket Name and Object Key both are null"));
    }
};

/**
 * This method will be invoked on file get command
 * */
var open = function(fileName, mode, callback){
    var opt = {fn: fileName};
    callback(null, opt);
};

var close = function(){

};

/**
 * This method will be invoked when a directory is deleted
 * */
var rmDir = function(path,callback){
    var pathObject = this.parseFtpPathUtil(path);
    var self= this;
    if(pathObject.bucketName != null && pathObject.objectKey == null){
        //The Command is to Delete bucket
        this.s3adapter.DeleteBucket(pathObject.bucketName,function(err,data){
            if(err){
                logger.error('ftp:rmDir: Error occurred while  deleting Bucket. Err- '+err);
            }
            callback(err);
        });
    }
    else if(pathObject.bucketName != null && pathObject.objectKey != null){
        //The command is to delete object under the bucket
        //Append trailing "/" in the object key (if not already exists) to delete folder in S3
        if(this.endsWithUtil(pathObject.objectKey,s3PathSeparator) == false){
            pathObject.objectKey += s3PathSeparator;
        }
        //Get all other files in this folder
        self.s3adapter.ListObjects(pathObject.bucketName,pathObject.objectKey,function(err,data){
            var objectsToDelete = [];
            if(!err){
                data.Contents.forEach(function(object){
                    objectsToDelete.push(object.Key);
                });
                self.s3adapter.deleteObjects(pathObject.bucketName,objectsToDelete,function(err,data){
                    if(err){
                        logger.error('ftp:rmDir: Error occurred while  deleting folder. Err- '+err);
                    }
                    callback(err);
                });
            }
            else{
                logger.error('ftp:rmDir: Error occurred while listing folders to delete. Err- '+err);
				callback(err);
            }
        })
    }
    else{
        logger.error('ftp:rmDir: Bucket Name and Object Key both are null');
        callback(new Error("Bucket Name and Object Key both are null"));
    }
};

/**
 * This method will be invoked on file rename command
 * */
var rename = function(fileFrom, fileTo, callback){
    var fromPathObject = this.parseFtpPathUtil(fileFrom);
    var toPathObject = this.parseFtpPathUtil(fileTo);
    var self = this;
    //Get the source object detail
    self.s3adapter.GetObjectDetail(fromPathObject.bucketName,fromPathObject.objectKey,function(err,data){
        if(!err){
            //Object found in S3, and it is a file hence copy it.
            var copySource = encodeURI(fromPathObject.bucketName + s3PathSeparator + fromPathObject.objectKey);
            self.s3adapter.CopyObject(toPathObject.bucketName,copySource,toPathObject.objectKey,function(err,data){
                if(!err){
                    self.s3adapter.DeleteObject(fromPathObject.bucketName,fromPathObject.objectKey,function(err,data){
                        if(err){
                            logger.error('ftp:rename:delete: Error renaming file. Err- '+err);
                        }
                        //Callback in either case, i.e. delete success of failure.
                        callback(err);
                    })
                }
                else{
                    //Copy failed, Callback with failure.
                    logger.error('ftp:rename:copy: Error renaming file. Err- '+err);
                    callback(err);
                }
            });
        }
        else{
            //Didn't find object in S3, It can be a directory, hence find all the Object in this directory and rename them.
            var prefix = fromPathObject.objectKey + s3PathSeparator;
            self.s3adapter.ListObjects(fromPathObject.bucketName,prefix,function(err,data){
                if(err){
                    //Didn't find anything with given prefix from S3 hence calling back with error
                    logger.error('ftp:rename:list: Error renaming file. Err- '+err);
                    callback(err);
                }
                else{
                    //Iterate through each object returned from S3 and prepare list of source and destination object key list.
                    var objectsToDelete =[];
                    var totalObjectsToProcess = data.Contents.length;
                    var processedObjectCount = 0;
                    data.Contents.forEach(function(obj){
                        var sourceObjectKey = obj.Key;
                        var sourceObjectPath = encodeURI(fromPathObject.bucketName + s3PathSeparator + obj.Key);
                        var desiObjectKey = obj.Key.replace(prefix, toPathObject.objectKey+s3PathSeparator);
                        self.s3adapter.CopyObject(toPathObject.bucketName,sourceObjectPath,desiObjectKey,function(err,data){
                            processedObjectCount += 1;
                            if(!err){
                                objectsToDelete.push(sourceObjectKey)
                            }

                            //Check if all copyObject requests are finished.
                            if(processedObjectCount == totalObjectsToProcess){
                                if(objectsToDelete.length > 0){
                                    self.s3adapter.deleteObjects(fromPathObject.bucketName,objectsToDelete,function(err,data){
                                        if(err){
                                            logger.error('ftp:rename:delete: Error renaming file. Err- '+err);
                                        }
                                        callback(err);
                                    });
                                }
                                else{
                                    logger.error('ftp:rename:delete: Did not find objects to delete');
                                    callback(new Error("Unable to rename object"));
                                }
                            }
                            else{
                                logger.info('ftp:rename:copycallback: Items to process:' + totalObjectsToProcess + ', Processed:-' + processedObjectCount);
                            }
                        });
                    });
                }
            });
        }
    });
};

/**
 * This method will be invoked if useWriteFile flag is not set or false
 * It will return a writable stream
 * */
var createWriteStream = function(fileName, streamFlags){
    var pathObject = this.parseFtpPathUtil(fileName);
    return this.s3adapter.UploadObject(pathObject.bucketName,pathObject.objectKey);
};

/**
 * This method will be invoked if useWriteFile flag is set to True
 * It will invoke createWriteStream method if file to be upload is greater than uploadMaxSlurpSize
 * */
var writeFile = function(fileName, data, callback){
    var wStreamFlags = {flags: "w", mode: 0644};
    var storeStream = this.createWriteStream(fileName, wStreamFlags);
    if (data) {
        storeStream.write(data);
    }
    storeStream.end();
    callback(null);
};

/**
 * This method will be invoked on file get command if useReadFile flag is not set or False
 * It will return a readable stream of content
 * */
var createReadStream = function(unknown,options){
    var pathObject = this.parseFtpPathUtil(options.fd.fn);
    return this.s3adapter.GetObject(pathObject.bucketName,pathObject.objectKey);
};

/**
 * This method will be invoked if useReadFile flag is set to True
 * It will callback with file contents to be sent to ftp client
 * */
var readFile = function(fileName,callback){
    var contents = null;
    callback(new Error("Method not implemented!"),contents);
};

var customFsImplementation = {
    unlink : unLink,
    readdir : readDir,
    mkdir : mkDir,
    open : open,
    close : close,
    rmdir : rmDir,
    rename : rename,
    stat :stat,
    createWriteStream : createWriteStream,
    writeFile : writeFile,
    createReadStream : createReadStream,
    readFile : readFile,
    user : null,
    s3adapter : null,
    connection : null,
    statCounter : 0,
    parseFtpPathUtil : parseFtpPath,
    getStatsUtil : getStats,
    endsWithUtil : endsWith,
    localStore :{
        buckets : [],
        objects :[]
    }
};

/******************************** [END] Custom Implementation for FS Operations ****************************************/



/**
 * Method to start FTP Server
 * */
exports.Start = function () {
    server = new ftpd.FtpServer(process.env.FTP_SERVER_IP || '127.0.0.1', {
        getInitialCwd: function () {
            return '';
        },
        getRoot: function () {
            return '';
        },
        //pasvPortRangeStart: 1025,
        //pasvPortRangeEnd: 1050,
        //tlsOptions: null,
        //allowUnauthorizedTls: true,
        useWriteFile: true,
        //useReadFile: false,
        uploadMaxSlurpSize: 1000000, // N/A unless 'useWriteFile' is true.

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
            if (pass) {
                var s3Adapter = require('../s3/s3adapter.js');
                s3Adapter.Initialize(username,pass);
                //s3Adapter.Initialize(config.AccessKeyId,config.SecretKey);
                customFsImplementation.s3adapter = s3Adapter;
                customFsImplementation.user = username;
                customFsImplementation.connection = this;
                success(username, customFsImplementation);
            } else {
                failure();
            }
        });
    });

    server.debugging = 0;
    server.listen(process.env.FTP_SERVER_PORT || 7002);
    console.log('FTP Server listening on IP:-' + process.env.FTP_SERVER_IP || '127.0.0.1' + ', Port:-' + process.env.FTP_SERVER_PORT || 7002);
    logger.info('Server listening on IP:- ' + process.env.FTP_SERVER_IP || '127.0.0.1' +   ', Port - ' + process.env.FTP_SERVER_PORT || 7002 );
};