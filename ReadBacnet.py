import asyncio
import BAC0
import paho.mqtt.client as mqtt
import json

# BACnet device configuration
device_id = 1004  # BACnet device address
bacnet_ip = "192.168.1.75"  # IP address of BACnet device
bacnet_port = 47808  # Standard BACnet port

# MQTT Configuration
MQTT_BROKER = "broker.hivemq.com"  # Replace with your MQTT broker address
MQTT_PORT = 1883
MQTT_TOPIC = "89_ai1"  # Topic to publish BACnet values

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

async def read_bacnet_values(bacnet_connection, mqtt_publisher):
    try:
        # List of objects to read (adjust according to your sensor mapping)
        objects_to_read = [
            ('analogInput', 1),  # Modify this to match your specific object
        ]
        
        # Read and store values
        values = {}
        for obj_type, obj_inst in objects_to_read:
            try:
                # Construct the full BACnet address
                full_address = f"{bacnet_ip} {obj_type} {obj_inst} presentValue"
                
                # Read the present value
                value = await bacnet_connection.read(full_address)
                values[f"{obj_type}-{obj_inst}"] = value
                print(f"{obj_type}-{obj_inst}: {value}")
                
                # Publish to MQTT
                mqtt_publisher.publish(MQTT_TOPIC, {
                    "type": obj_type,
                    "instance": obj_inst,
                    "value": value
                })
            
            except Exception as obj_error:
                print(f"Error reading {obj_type}-{obj_inst}: {obj_error}")
        
        return values
    
    except Exception as e:
        print(f"General error in reading BACnet values: {e}")
        return {}

async def main():
    # Initialize MQTT Publisher
    mqtt_publisher = MQTTPublisher(MQTT_BROKER, MQTT_PORT)
    
    try:
        # Initialize the BAC0 connection asynchronously
        async with BAC0.connect(ip="192.168.1.21", port=1099) as bacnet_connection:
            # Main reading loop
            while True:
                await read_bacnet_values(bacnet_connection, mqtt_publisher)
                await asyncio.sleep(1)  # Read every 1 second
    
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