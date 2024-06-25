/*
 * Microsoft Sample Code - Copyright (c) 2020 - Licensed MIT
 */

const { EventHubConsumerClient } = require('@azure/event-hubs');
const { convertIotHubToEventHubsConnectionString } = require('./iot-hub-connection-string.js');

class EventHubReader {
  constructor(iotHubConnectionString, consumerGroup) {
    this.iotHubConnectionString = iotHubConnectionString;
    this.consumerGroup = consumerGroup;
    this.consumerClient = null;
  }

  async startReadMessage(startReadMessageCallback) {
    try {
      const eventHubConnectionString = await convertIotHubToEventHubsConnectionString(this.iotHubConnectionString);
      this.consumerClient = new EventHubConsumerClient(this.consumerGroup, eventHubConnectionString);
      console.log('Successfully created the EventHubConsumerClient from IoT Hub event hub-compatible connection string.');

      const partitionIds = await this.consumerClient.getPartitionIds();
      console.log('The partition ids are: ', partitionIds);

      this.consumerClient.subscribe({
        processEvents: (events, context) => {
          for (let i = 0; i < events.length; ++i) {
            startReadMessageCallback(
              events[i].body,
              events[i].enqueuedTimeUtc,
              events[i].systemProperties["iothub-connection-device-id"]
            );
          }
        },
        processError: (err, context) => {
          console.error(err.message || err);
        }
      });
    } catch (ex) {
      console.error(ex.message || ex);
    }
  }

  async stopReadMessage() {
    try {
      if (this.consumerClient) {
        await this.consumerClient.close();
        console.log('EventHubConsumerClient closed.');
      } else {
        console.warn('No EventHubConsumerClient to close.');
      }
    } catch (ex) {
      console.error('Error closing EventHubConsumerClient:', ex.message || ex);
    }
  }
}

module.exports = EventHubReader;
