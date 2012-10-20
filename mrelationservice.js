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

var ANSWER = "Answer"

var server = exports.relation = thrift.createServer(RelationService,{

    getMsgCounter: function(mids,response){
        var ret = []
        process.log.info(util.format("getMsgCounter:mids=%s",mids))
        pool.borrow(function(err,client){
            mids.forEach(function(ele){
                 client.hget(ele,ANSWER,function(err,reply){
                     if(err!=null){
                         ret[ele.toString]=-1
                     }else{
                         ret[ele.toString]=reply;
                     }
                     if(ret.length==mids.length){
                        pool.release(client)
                        response(ret);
                         ret = null;
                     }
                 })
            })
        })
    },

    getRelatedMsg:function(mid,start,len,response){
        process.log.info(util.format("getRelatedMsg:mid=%s,start=%d,len=%d",mid,start,len))
        pool.borrow(function(err,client){
            client.zrevrange(ANSWER+"_"+mid,start-1,start+len-1,function(err,reply){
                 pool.release(client)
                 response(reply)
            })
        })
    },

    initMsg:function(mid,response){
        process.log.info(util.format("initMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.hset(mid,ANSWER,"0",function(err,reply){
                client.zadd(ANSWER+"_"+mid,0,"-1",function(err,reply){
                    pool.release(client);
                    if(err!=null)
                        response(FALSE)
                    else
                        response(TRUE);
                });
            });
        })
    } ,

    incr:function(mid,response){
        process.log.info(util.format("incrMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.hincrby(mid,ANSWER,1,function(err,reply){
                pool.release(client)
                if(err!=null)
                    response(FALSE)
                else
                    response(TRUE)
            });
        })
    },

    decr:function(mid,response){
        process.log.info(util.format("decrMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.hincrby(mid,ANSWER,1,function(err,reply){
                pool.release(client)
                if(err!=null)
                    response(FALSE)
                else
                    response(TRUE)
            });
        })
    },

    addRelatedMsg:function(mid,answer,response){
        process.log.info(util.format("addRelatedMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.zadd(ANSWER+"-"+mid,0,answer,function(err,reply){
                client.zremrangebyrank(ANSWER+"_"+msg.getRelatedmid(),process.conf.server.topN,-1,function(err,reply){
                    pool.release(client)
                    if(err!=null){
                        response(FALSE)
                        return;
                    }
                    incr(mid,function(reply){
                        response(reply)
                    });
                });
            });
        })
    },

    deleteRelatedMsg:function(mid,answer,response){
        process.log.info(util.format("deleteRelatedMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.zrem(ANSWER+"-"+mid,answer,function(err,reply){
                pool.release(client)
                if(err!=null){
                    response(FALSE)
                    return
                }
                decr(mid,function(reply){
                    response(reply)
                });
            });
        })
    }
})

