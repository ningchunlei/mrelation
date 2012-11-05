var thrift = require("thrift")
var util = require("util");
var RelationService = require("./thrift/MRelationIFace")
var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")
var IKaoDBService = require("./thrift/IKaoDBIFace")
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

var ikaodbPool = poolModule.Pool({
    name : "ikaodb",
    create   : function(callback) {
        var dbconnection = thrift.createConnection(process.conf.db.ip, process.conf.db.port)
        var client = thrift.createClient(IKaoDBService, dbconnection);
        callback(null, client);
    },
    destroy  : function(client) { client.quit(); }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
})

var FALSE = -1
var TRUE = 1

var ANSWER = "Answer"
var RIGHT = "Right"

var server = exports.relation = thrift.createServer(RelationService,{

    getMsgCounter: function(mids,response){
        var ret = []
        var icount = 0
        process.log.info(util.format("getMsgCounter:mids=%s",mids))
        pool.borrow(function(err,client){
            mids.forEach(function(ele){
                 client.hgetall(ele,function(err,reply){
                     icount++;
                     if(reply!=null && reply.length>0){
                         ret[ele.toString]=reply;
                     }
                     if(icount==mids.length){
                        pool.release(client)
                        if(ret.length!=mids.length){
                            ikaodbPool.borrow(function(err,idb){
                                idb.getMsgCounter(mids,function(err,reply){
                                    ikaodbPool.release(idb)
                                    response(reply);
                                })
                            })
                            return;
                        }
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
                 if(reply!=null && reply.length==0){
                     ikaodbPool.borrow(function(err,idb){
                           idb.getRelatedMsg(mid,start,len,function(err,reply){
                               ikaodbPool.release(idb)
                               response(reply)
                           })
                     })
                 }else{
                    response(reply)
                 }
            })
        })
    },

    initMsg:function(mid,response){
        process.log.info(util.format("initMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.hmset(mid,ShareStruct_ttypes.MsgCounter.answer,0,ShareStruct_ttypes.MsgCounter.right,0,function(err,reply){
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

    incr:function(mid,type,response){
        process.log.info(util.format("incrMsg:mid=%s,type=%d",mid,type))
        pool.borrow(function(err,client){
            client.exists(mid,function(err,reply){
                ikaodbPool.borrow(function(err,idb){
                    idb.incrForMsg(mid,type,function(){ikaodbPool.release(idb)})
                })
                if(reply==1){
                    client.hincrby(mid,type,1,function(err,reply){
                        pool.release(client)
                        if(err!=null)
                            response(FALSE)
                        else
                            response(TRUE)
                    });
                }else{
                    pool.release(client)
                    response(TRUE)
                }
            })
        })
    },

    decr:function(mid,type,response){
        process.log.info(util.format("decrMsg:mid=%s,type=%d",mid,type))
        pool.borrow(function(err,client){
            client.exists(mid,function(err,reply){
                ikaodbPool.borrow(function(err,idb){
                    idb.decrForMsg(mid,type,function(){ikaodbPool.release(idb)})
                })
                if(reply==1){
                    client.hincrby(mid,type,-1,function(err,reply){
                        pool.release(client)
                        if(err!=null)
                            response(FALSE)
                        else
                            response(TRUE)
                    });
                }else{
                    pool.release(client)
                    response(TRUE)
                }
            })
        })
    },

    addRelatedMsg:function(mid,answer,response){
        process.log.info(util.format("addRelatedMsg:mid=%s",mid))
        pool.borrow(function(err,client){
            client.exists(ANSWER+"-"+mid,function(err,reply){
                ikaodbPool.borrow(function(err,idb){
                    idb.addRelatedMsg(mid,answer,function(){ikaodbPool.release(idb)})
                })
                if(reply==1){
                    client.zadd(ANSWER+"-"+mid,new Date().getTime(),answer,function(err,reply){
                        pool.release(client);
                        incr(mid,ShareStruct_ttypes.MsgCounter.answer,function(){response(TRUE)})
                    });
                }else{
                    pool.release(client);
                    incr(mid,ShareStruct_ttypes.MsgCounter.answer,function(){response(TRUE)})
                }
            })

        })
    },

    deleteRelatedMsg:function(mid,answer,response){
        process.log.info(util.format("deleteRelatedMsg:mid=%s",mid))
        ikaodbPool.borrow(function(err,idb){
            idb.deleteRelatedMsg(mid,answer,function(){ikaodbPool.release(idb)})
        })
        pool.borrow(function(err,client){
            client.zrem(ANSWER+"-"+mid,answer,function(err,reply){
                pool.release(client)
                if(err!=null){
                    response(FALSE)
                    return
                }
                decr(mid,ShareStruct_ttypes.MsgCounter.answer,function(reply){
                    response(reply)
                });
            });
        })
    }
})

