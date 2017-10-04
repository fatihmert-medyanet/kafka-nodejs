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
  
  var readline = require('readline');
  var avro = require('avsc');
  var type = avro.parse(typeDescription);

  var kafka = require('kafka-node');
  var HighLevelProducer = kafka.HighLevelProducer;
  var KeyedMessage = kafka.KeyedMessage;
  var Client = kafka.Client;

  var client = new Client('localhost:2181', 'my-client-id', {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
  });
  
  // For this demo we just log client errors to the console.
  client.on('error', function(error) {
    console.error(error);
  });

  var producer = new HighLevelProducer(client);

  producer.on('ready', function() {
    // Create message and encode to Avro buffer

    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      });
      
      rl.question('What do you want to say: ', (answer) => {
        // TODO: Log the answer in a database
        var messageBuffer = type.toBuffer({
            enumField: 'sym1',
            id: 'acsds-3434dfd-34dfdf',
            timestamp: Date.now(),
            message: answer
          });
        
          // Create a new payload
          var payload = [{
            topic: 'node-topic',
            messages: messageBuffer,
            attributes: 1 /* Use GZip compression for the payload */
          }];
        
          //Send payload to Kafka and log result/error
          producer.send(payload, function(error, result) {
            console.info('Sent payload to Kafka: ', payload);
            if (error) {
              console.error(error);
            } else {
              var formattedResult = result[0]
              console.log('result: ', result)
            }
          });
      
        rl.close();
      });

    
  });
  
  // For this demo we just log producer errors to the console.
  producer.on('error', function(error) {
    console.error(error);
  });


  
  