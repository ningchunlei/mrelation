var thrift = require('thrift');

var TimeLineService = require("./thrift/TimeLineIFace")
var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")

var connection = thrift.createConnection('localhost', 8090),
    client = thrift.createClient(TimeLineService, connection);

connection.on('error', function(err) {
    console.error(err);
});

client.add("ningcl",[ShareStruct_ttypes.TimeLineType.Inbox],"1231431232",function(err,response){
    console.log("add"+response)
})

client.get("ningcl",ShareStruct_ttypes.TimeLineType.Inbox,1,4,function(err,reponse){
    console.log(reponse.mids)
})



client.remove("ningcl","123143",ShareStruct_ttypes.TimeLineType.Inbox,function(err,response){
    console.log("rem"+response)
})

