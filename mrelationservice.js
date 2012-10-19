var thrift = require("thrift")
var util = require("util");
var RelationService = require("./thrift/MRelationIFace")
var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")
var redis = require("redis")

var poolModule = require('generic-pool');
var pool = poolModule.Pool({
    name     : 'redis',
    create   : function(callback) {
        var client = redis.createClient(process.conf.redis.port,process.conf.redis.ip);
        client.auth(process.conf.redis.passwd)
        callback(null, client);
    },
    destroy  : function(client) { client.quit(); }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
});

var FALSE = -1
var TRUE = 1


var server = exports.relation = thrift.createServer(RelationService,{

    getMsgCounter: function(mids,response){

    },

    getRelatedMsg:function(mid,start,len,response){

    },

    initMsg:function(mid,response){

    } ,

    incr:function(mid,response){

    },

    decr:function(mid,response){

    },

    addRelatedMsg:function(mid,answer,response){

    },

    deleteRelatedMsg:function(mid,answer){

    }
})

