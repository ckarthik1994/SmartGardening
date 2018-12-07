# Example of low level interaction with a BLE UART device that has an RX and TX
# characteristic for receiving and sending data.  This doesn't use any service
# implementation and instead just manipulates the services and characteristics
# on a device.  See the uart_service.py example for a simpler UART service
# example that uses a high level service implementation.
# Author: Tony DiCola
import logging
import time
import uuid

import Adafruit_BluefruitLE

import boto3
import datetime
import os
import pymysql

aws_access_id='###'
aws_secret_key='###'


# Enable debug output.
#logging.basicConfig(level=logging.DEBUG)

# Define service and characteristic UUIDs used by the UART service.
SOIL_MOISTURE_SENSOR_SERVICE_UUID = uuid.UUID('358824f8-8ae8-4dfa-8890-ffc97bb65130')
SOIL_MOISTURE_SENSOR_CHAR_UUID    = uuid.UUID('8793e6c1-c427-4e92-bbd9-d752ec720e61')

# Get the BLE provider for the current platform.
ble = Adafruit_BluefruitLE.get_provider()

connection = pymysql.connect(host='###',
                             user='###',
                             password='###',
                             db='###',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


# Main function implements the program logic so it can run in a background
# thread.  Most platforms require the main thread to handle GUI events and other
# asyncronous events like BLE actions.  All of the threading logic is taken care
# of automatically though and you just need to provide a main function that uses
# the BLE provider.
def main():
    # Clear any cached data because both bluez and CoreBluetooth have issues with
    # caching data and it going stale.
    ble.clear_cached_data()

    # Get the first available BLE network adapter and make sure it's powered on.
    adapter = ble.get_default_adapter()
    adapter.power_on()
    print('Using adapter: {0}'.format(adapter.name))

    # Disconnect any currently connected UART devices.  Good for cleaning up and
    # starting from a fresh state.
    print('Disconnecting any connected UART devices...')
    ble.disconnect_devices([SOIL_MOISTURE_SENSOR_SERVICE_UUID])

    # Scan for UART devices.
    print('Searching for UART device...')
    try:
        adapter.start_scan()
        # Search for the first UART device found (will time out after 60 seconds
        # but you can specify an optional timeout_sec parameter to change it).
        device = ble.find_device(name='Bluefruit SMSense')
        if device is None:
            raise RuntimeError('Failed to find UART device!')
    finally:
        # Make sure scanning is stopped before exiting.
        adapter.stop_scan()

    print('Connecting to device...')
    device.connect()  # Will time out after 60 seconds, specify timeout_sec parameter
                      # to change the timeout.

    # Once connected do everything else in a try/finally to make sure the device
    # is disconnected when done.
    try:
        # Wait for service discovery to complete for at least the specified
        # service and characteristic UUID lists.  Will time out after 60 seconds
        # (specify timeout_sec parameter to override).
        print('Discovering services...')
        device.discover([SOIL_MOISTURE_SENSOR_SERVICE_UUID], [SOIL_MOISTURE_SENSOR_CHAR_UUID])

        # Find the UART service and its characteristics.
        uart = device.find_service(SOIL_MOISTURE_SENSOR_SERVICE_UUID)
        rx = uart.find_characteristic(SOIL_MOISTURE_SENSOR_CHAR_UUID)

        # Function to receive RX characteristic changes.  Note that this will
        # be called on a different thread so be careful to make sure state that
        # the function changes is thread safe.  Use queue or other thread-safe
        # primitives to send data to other threads.
        def received(data):
            sensor_val = int(float(format(data)))
            print('Received: %f', sensor_val)
            d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `moisture_sensor` (`timestamp`, `moisture`) VALUES (%s, %s)"
                cursor.execute(sql, (d, sensor_val))
            connection.commit()

        # Turn on notification of RX characteristics using the callback above.
        print('Subscribing to RX characteristic changes...')
        rx.start_notify(received)

        # Now just wait for 30 seconds to receive data.
        print('Waiting 60 seconds to receive data from the device...')
        time.sleep(60)

        while True:
            time.sleep(1)
    except Exception as e:
        print(e)
    finally:
        # Make sure device is disconnected on exit.
        device.disconnect()
        connection.close()


# Initialize the BLE system.  MUST be called before other BLE calls!
ble.initialize()

# Start the mainloop to process BLE events, and run the provided function in
# a background thread.  When the provided main function stops running, returns
# an integer status code, or throws an error the program will exit.
ble.run_mainloop_with(main)
