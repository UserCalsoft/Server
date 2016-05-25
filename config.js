/** Module to keep application configurations */

var Server_Port = process.env.FTP_SERVER_PORT;
var File_List_Count_Limit = 100;
var S3_Server_EndPoint ="https://s3.amazonaws.com";


exports.ServerPort = Server_Port;
exports.FileListCountLimit = File_List_Count_Limit;
exports.S3ServerEndPoint = S3_Server_EndPoint;
