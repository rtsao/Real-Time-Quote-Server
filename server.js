var http = require('http');
var redis = require("redis");

var serverPublisher = redis.createClient();
var serverRedis = redis.createClient();

function pubb() {
    getQuotes();
    /*
    serverPublisher.hkeys('tickers', function (err, obj) {
        console.log('publishing',obj);
        obj.forEach(function(element) {
            serverPublisher.publish(element,'Yo!', function(err,res) {
                if (!res) {
                    prune(element);
                }
            });
        });
    });
    */
}

function getQuotes() {
    serverPublisher.hkeys('tickers', function (err, obj) {
        console.log('publishing',obj);
        var tickerString = obj.join(",");

        http.get({
            host: 'www.google.com',
            port: 80,
            path: '/finance/info?infotype=infoquoteall&q=' + tickerString
        }, function(response) {
            response.setEncoding('utf8');
            var data = "";
                        
            response.on('data', function(chunk) {
                data += chunk;
            });
            
            response.on('end', function() {
                if(data.length > 0) {
                    try {
                        var quotes = JSON.parse(data.substring(3));
                    } catch(e) {
                        return false;
                    }

                    //quotes.forEach(publishQuote);
                    quotes.forEach(function(element) {
                        var quote = JSON.stringify(element);

                        serverRedis.hget('quotes',element.e+':'+element.t,function(err, reply) {
                            if (reply !== quote) {
                                serverRedis.hset('quotes',element.e+':'+element.t,quote);
                                publishQuote(element);
                            }
                        });

                    });

                }
            });
        });


    });
}

function publishQuote(quote) {
    serverPublisher.publish(quote.e+':'+quote.t, JSON.stringify(quote), function(err,res) {
        if (!res) {
            prune(quote.e+':'+quote.t);
        }
    });
}

function prune(ticker) {
    serverRedis.watch('tickers',ticker);
    serverRedis.hget('tickers',ticker,function(err, reply) {
        multi = serverRedis.multi();
        if (reply<1) {
            multi.hdel('tickers',ticker);
            multi.hdel('quotes',ticker);
            console.log('deleting',ticker);
        }
        multi.exec(redis.print);
    });
}

var timer = setInterval(getQuotes, 15000);

var WebSocketServer = require('ws').Server
var wss = new WebSocketServer({port: 1337});

wss.on('connection', function(socket) {
    socket.send('you have connected');

    var sendMessage = function (channel, message) {
        console.log('message heard!');
        socket.send(message);
    }
    
    var stockList = [];
    var userClient = redis.createClient();

    //Send subscriptions to user
    userClient.on("message", sendMessage);

    socket.on('message', function(message) {
        
        console.log('received: %s', message);

        try {
            data = JSON.parse(message)
        } catch(e) {
            socket.close(1003, 'malformed json');
            return;
        }

        if (Array.isArray(data.subscribe)) {
            data.subscribe.forEach(function(ticker) {
                if (stockList.indexOf(ticker) === -1) {
                    verify_stock(ticker,stockList,serverRedis,userClient);
                }
            });
        }

        if (Array.isArray(data.unsubscribe)) {
            data.unsubscribe.forEach(function(ticker) {
                var index = stockList.indexOf(ticker);
                if (index !== -1) {
                    stockList.splice(index,1);
                    //serverRedis.hincrby('tickers',ticker,-1);
                    removeStock(ticker);
                    userClient.unsubscribe(ticker);
                }
            });
        }


        //
        //Still need to validate that stocks are valid. For now assume okay
        //


    });

    socket.on('close', function() {

        console.log('connection closed');
        
        //Decrement stocks from db
        removeStocks(stockList);

        if (userClient) {
            //userClient.unsubscribe();
            userClient.end(redis.print);
        }
        
    });

});



function addStock(ticker) {
    //Increment stock ticker
    serverRedis.hincrby('tickers',ticker,1);
}

function addStocks(list,client) {
    //client.subscribe(list, redis.print);
    list.forEach(addStock);
}

function removeStock(ticker) {
    //Decrement stock ticker
    serverRedis.hincrby('tickers',ticker,-1, function(err,reply) {
        if (reply<=0) {
            prune(ticker);
        }
        console.log(reply);
    });
}

function removeStockAtomic(ticker) {
    console.log(ticker);
    serverRedis.hincrby('tickers',ticker,-1);
    console.log('watch',ticker);
    serverRedis.watch('tickers',ticker);
    serverRedis.hget('tickers',ticker,function(err, reply) {
        console.log('reply',reply);
        multi = serverRedis.multi();
        if (reply<1) {
            console.log('deleting',ticker);
            multi.hdel('tickers',ticker);
            console.log('exec',ticker)
        }
        multi.exec(redis.print);
    });

}

function removeStocks(list) {
    list.forEach(removeStock);
}


function verify_stock(p_ticker, list, server, client) {
    http.get({
        host: 'www.google.com',
        port: 80,
        path: '/finance/info?client=ig&q=' + p_ticker
    }, function(response) {
        response.setEncoding('utf8');
        var data = "";
                    
        response.on('data', function(chunk) {
            data += chunk;
        });
        
        response.on('end', function() {
            if(data.length > 0) {
                try {
                    var quote = JSON.parse(data.substring(3))[0];
                } catch(e) {
                    return false;
                }
                console.log(quote);
                var validTicker=quote.e;
                validTicker+=':';
                validTicker+=quote.t;
                console.log(p_ticker,'validticker?',validTicker === p_ticker);
                if (validTicker === p_ticker) {
                    list.push(p_ticker);
                    server.hincrby('tickers',p_ticker,1);
                    client.subscribe(p_ticker);
                }
            }
        });
    });
}

function valid_ticker(p_ticker) {

    http.get({
        host: 'www.google.com',
        port: 80,
        path: '/finance/info?client=ig&q=' + p_ticker
    }, function(response) {
        response.setEncoding('utf8');
        var data = "";
                    
        response.on('data', function(chunk) {
            data += chunk;
        });
        
        response.on('end', function() {
            if(data.length > 0) {
                try {
                    var quote = JSON.parse(data.substring(3))[0];
                } catch(e) {
                    return false;
                }
                console.log(quote);
                var validTicker=quote.e;
                validTicker+=':';
                validTicker+=quote.t;
                console.log('validTicker',validTicker);
                console.log('p_ticker:',p_ticker);
                console.log(validTicker === p_ticker);
                return (validTicker === p_ticker);
            }
        });
    });
}

function matches(ticker) {
    var validTicker=quote.e;
    validTicker+=':';
    validTicker+=quote.t;
    console.log('validTicker',validTicker);
    console.log('p_ticker:',p_ticker);
    console.log(validTicker === p_ticker);
    return (validTicker === p_ticker);
}