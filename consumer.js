var typeDescription = {
    name: 'MessageType',
    type: 'record',
    fields: [{
      name: 'enumField',
      type: {
        name: 'EnumField',
        type: 'enum',
        symbols: ['sym1', 'sym2', 'sym3']
      }
    }, {
      name: 'id',
      type: 'string'
    }, {
      name: 'timestamp',
      type: 'double'
    },{
        name: 'message',
        type: 'string'
    }]
  };
  

var avro = require('avsc');
var type = avro.parse(typeDescription);

var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;

var client = new Client('localhost:2181','my-client-id',{
    sessionTimeout: 300,
    spinDelay:100,
    retries:2
});

var topics = [{
    topic:'node-topic'
}];

var options = {
    autoCommit: true,
    fetchMaxWaitMs: 1000,
    fetchMaxBytes: 1024 * 1024,
    encoding:'buffer'
};

var consumer = new HighLevelConsumer(client,topics,options);

consumer.on('message', function(message) {
    var buf = new Buffer(message.value, 'binary'); // Read string into a buffer.
    var decodedMessage = type.fromBuffer(buf.slice(0)); // Skip prefix.
    console.log(decodedMessage);
  });
  
  consumer.on('error', function(err) {
    console.log('error', err);
  });

process.on('SIGINT',function(){
    consumer.close(true,function(){
        process.exit();
    });
});