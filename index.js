var redis = require('redis');

module.exports.attach = function (broker) {
  var brokerOptions = broker.options.brokerOptions;
  var instanceId = broker.instanceId;
  var cluster = false
  var subClients = []
  var index_to_use = 0

  var nextOne = function() {
    if (index_to_use >= subClients.length-1) { index_to_use = 0 }
    else { index_to_use++  }
    handleRedisMessage()
  }

  var handleRedisMessage = function() {

    subClients[index_to_use].on('message', function (channel, message) {
      var sender = null;
      message = message.replace(instanceIdRegex, function (match) {
        sender = match.slice(0, -1);
        return '';
      });
      
      // Do not publish if this message was published by 
      // the current SC instance since it has already been
      // handled internally
      if (sender == null || sender != instanceId) {
        var type = message.charAt(0);
        var data;
        if (type == 'o') {
          try {
            data = JSON.parse(message.slice(2));
          } catch (e) {
            data = message.slice(2);
          }
        } else {
          data = message.slice(2);
        }
        broker.publish(channel, data);
      }
    });

    subClients[index_to_use].on('end',function() {
      console.log('    Redis Slave error: Getting next slave connection...')
      subClients[index_to_use].off('message')
      nextOne()
    })
  }

  if (brokerOptions.slaves && brokerOptions.slaves.length) {
      var slaves = brokerOptions.slaves

      slaves.forEach(function(slave,index) {
        if (typeof slave == 'object' && slave.host && slave.port) {
          subClients.push(redis.createClient(slave.port, slave.host, brokerOptions))
        }
      })
    }
  }

  if (!subClients.length) { subClients.push(redis.createClient(brokerOptions.port, brokerOptions.host, brokerOptions)) }
  var pubClient = redis.createClient(brokerOptions.port, brokerOptions.host, brokerOptions);


  broker.on('subscribe', subClient.subscribe.bind(subClient));
  broker.on('unsubscribe', subClient.unsubscribe.bind(subClient));
  broker.on('publish', function (channel, data) {
    if (data instanceof Object) {
      try {
        data = '/o:' + JSON.stringify(data);
      } catch (e) {
        data = '/s:' + data;
      }
    } else {
      data = '/s:' + data;
    }
    
    if (instanceId != null) {
      data = instanceId + data;
    }
    
    pubClient.publish(channel, data);
  });
  
  var instanceIdRegex = /^[^\/]*\//;

  handleRedisMessage()
};