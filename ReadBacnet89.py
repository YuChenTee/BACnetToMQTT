import asyncio
import BAC0
import paho.mqtt.client as mqtt
import json

# BACnet device configuration
bacnet_ip = "192.168.1.89"  # IP address of BACnet device
device_ip = "192.168.1.21" # IP address of the device that runs this script
device_port = 1099 # Port Number of the device that runs this script (any port that is not used)

# MQTT Configuration
MQTT_BROKER = "broker.emqx.io"  # Replace with your MQTT broker address
MQTT_PORT = 1883
MQTT_TOPIC = "89_readings"  # Topic to publish BACnet values

# List of objects to read (adjust according to your sensor mapping)
objects_to_read = [('analogValue', i) for i in range(17)] + [('analogValue', i) for i in range(19, 26)]+ [('binaryValue', j) for j in range(2)]

class MQTTPublisher:
    def __init__(self, broker, port):
        self.client = mqtt.Client()
        self.broker = broker
        self.port = port
        
        try:
            self.client.connect(broker, port)
            print(f"Connected to MQTT Broker {broker}")
        except Exception as e:
            print(f"Failed to connect to MQTT Broker: {e}")
    
    def publish(self, topic, payload):
        try:
            # Convert payload to JSON if it's not already a string
            if not isinstance(payload, str):
                payload = json.dumps(payload)
            
            result = self.client.publish(topic, payload)
            
            # Check if publish was successful
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"Published to {topic}: {payload}")
            else:
                print(f"Failed to publish to {topic}")
        
        except Exception as e:
            print(f"MQTT Publish Error: {e}")
    
    def disconnect(self):
        self.client.disconnect()

async def read_bacnet_values(objects_to_read, bacnet_connection, mqtt_publisher):
    try:
        # Read and store values
        values = {}
        for obj_type, obj_inst in objects_to_read:
            try:
                # Construct the full BACnet address
                full_address = f"{bacnet_ip} {obj_type} {obj_inst} presentValue"
                
                # Read the present value
                value = await bacnet_connection.read(full_address)
                if obj_type == "analogValue":
                    values[f"av{obj_inst}"] = value
                else:
                    values[f"bv{obj_inst}"] = value
                # print(f"{obj_type}-{obj_inst}: {value}")
            
            except Exception as obj_error:
                print(f"Error reading {obj_type}-{obj_inst}: {obj_error}")
        
        # Publish to MQTT
        mqtt_publisher.publish(MQTT_TOPIC, values)

        return values
    
    except Exception as e:
        print(f"General error in reading BACnet values: {e}")
        return {}

async def main():
    # Initialize MQTT Publisher
    mqtt_publisher = MQTTPublisher(MQTT_BROKER, MQTT_PORT)
    
    try:
        # Initialize the BAC0 connection asynchronously
        async with BAC0.connect(ip=device_ip, port=device_port) as bacnet_connection:
            # Main reading loop
            while True:
                await read_bacnet_values(objects_to_read, bacnet_connection, mqtt_publisher)
                await asyncio.sleep(60)  # Read every 1 second
    
    except KeyboardInterrupt:
        print("Exiting...")
    except Exception as init_error:
        print(f"Failed to initialize BACnet connection: {init_error}")
    finally:
        # Ensure MQTT connection is closed
        mqtt_publisher.disconnect()

if __name__ == '__main__':
    # Set log level to reduce verbosity if needed
    BAC0.log_level('error')
    
    # Run the async main function
    asyncio.run(main())