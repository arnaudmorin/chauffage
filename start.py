#!/usr/bin/env python3
#
# Manage heat through MQTT
#
# Force a tele:
# cmnd/tasmota_9DA6D1/teleperiod "300"
# Force a result:
# cmnd/tasmota_9DA6D1/power
#
# Author: Arnaud Morin <arnaud.morin@gmail.com>
# License: Apache 2

import paho.mqtt.client as mqtt
import json
import logging
import pickle
import os
from collections import deque
from datetime import datetime as dt
import time

consigne = 21.8
hyst = 0.2


def read_states(f):
    # Read the previous states from a pickle
    if os.path.isfile(f):
        with open(f, 'rb') as fh:
            states = pickle.load(fh)
    else:
        # States is a deque, with max 100 states
        # So we avoid having too much data
        states = deque(maxlen=100)
    return states


def save_states(f, states):
    # Save the states in a pickle file
    with open(f, 'wb') as fh:
        pickle.dump(states, fh)


def is_confort(date):
    """Check if we are in confort period"""
    if date.hour >= 6 and date.hour < 8:
        return True
    if date.hour >= 19 and date.hour < 21:
        return True
    return False


class MqttClient(object):
    def __init__(self):
        # Init the logger
        self.logger = logging.getLogger('chauffage')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug('Starting logger')

        # Init the MQTT client
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set('tasmota', 'tasmota')    # Yes, this is the password
        self.logger.debug('Connecting to Mosquitto')
        self.client.connect("127.0.0.1", 1883, 60)

        # Queue
        self.queue_file = os.path.expanduser('~/.chauffage/states')
        self.queue = read_states(self.queue_file)

        # current status
        self.status = None
        self.command_sent = False
        self.forced_ts = 0

    def on_connect(self, client, userdata, flags, rc):
        '''
        The callback for when the client receives a CONNACK response from the server.
        '''
        self.logger.debug('Connected to Mosquitto')

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        # client.subscribe("+/+/+")
        self.logger.debug('Subscribing to tele/tasmota_9DA6D1/*')
        client.subscribe("tele/tasmota_9DA6D1/SENSOR")
        client.subscribe("stat/tasmota_9DA6D1/RESULT")

        # Get status
        self.command_sent = True
        client.publish("cmnd/tasmota_9DA6D1/POWER", "Hello world!")

    def on_message(self, client, userdata, msg):
        '''
        The callback for when a PUBLISH message is received from the server.
        '''
        try:
            message = json.loads(msg.payload)
            self.logger.debug(f'{msg.topic} | {message}')

            # Sensor data every five minutes
            if msg.topic == 'tele/tasmota_9DA6D1/SENSOR':
                # Timestamp
                try:
                    date = dt.strptime(message['Time'], "%Y-%m-%dT%H:%M:%S")
                except Exception as e:
                    self.logger.error('Error occured while parsing timestamp: {}'.format(e))

                # Temperature
                try:
                    temperature = message['SI7021']['Temperature']
                except Exception as e:
                    self.logger.error('Error occured while parsing temperature: {}'.format(e))

                # Humidity
                try:
                    humidity = message['SI7021']['Humidity']
                except Exception as e:
                    self.logger.error('Error occured while parsing humidity: {}'.format(e))

                # Save the state
                # NOTE: this is not necessary for now, but maybe useful in the future?
                # ca mange pas de pain
                if temperature and humidity and date:
                    self.logger.info(f'date={date} temperature={temperature} humidity={humidity}')
                    self.queue.append({'date': date, 'temperature': temperature, 'humidity': humidity})
                    save_states(self.queue_file, self.queue)

                    # Are we in forced period?
                    if self.forced_ts < dt.timestamp(date) - 3600:
                        self.logger.debug('Not forced / Automatic mode')
                        # Are we in confort period?
                        if is_confort(date):
                            self.logger.debug('Confort period')
                            # Are we already ON?
                            if self.status == 'ON':
                                # Are we hot enough?
                                if temperature > consigne + hyst:
                                    # Turn off
                                    self.logger.debug('Hot enough, shutting off')
                                    self.poweroff()
                            else:
                                # We are OFF
                                # Are we hot enough?
                                if temperature < consigne - hyst:
                                    self.logger.debug('Not hot enough, turning on')
                                    # Turn off
                                    self.poweron()
                        else:
                            # We are not in confort mode
                            self.poweroff()
                    else:
                        time_left = round((3600 - (dt.timestamp(date) - self.forced_ts))/60)
                        self.logger.debug(f'Manually forced to {self.status}. Time left: {time_left} minutes')

            # Button pressed
            if msg.topic == 'stat/tasmota_9DA6D1/RESULT':
                # Discard any other result
                if 'POWER' in message:
                    # If self.status is None, it's the first status we have
                    # There is a chance that it comes from our own check
                    if self.command_sent:
                        self.command_sent = False
                        self.status = message['POWER']
                        self.logger.info(f'Heater is {self.status}')
                    else:
                        # Someone press the button to force ON/OFF
                        self.status = message['POWER']
                        self.forced_ts = time.time()
                        self.logger.info(f'Heater is forced {self.status}')

        except Exception as e:
            self.logger.error('Error occured: {}'.format(e))

    def poweron(self):
        """turn on the heater"""
        self.logger.debug('Asking power ON')
        self.command_sent = True
        self.client.publish("cmnd/tasmota_9DA6D1/POWER", "ON")

    def poweroff(self):
        """turn off the heater"""
        self.logger.debug('Asking power OFF')
        self.command_sent = True
        self.client.publish("cmnd/tasmota_9DA6D1/POWER", "OFF")

    def serve(self):
        '''
        Blocking call that processes network traffic, dispatches callbacks and
        handles reconnecting.
        Other loop*() functions are available that give a threaded interface and a
        manual interface.
        '''
        self.logger.info('Starting to serve')
        self.client.loop_forever()


if __name__ == '__main__':
    mq = MqttClient()
    mq.serve()
