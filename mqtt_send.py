#!/usr/bin/env python3
import sys
import json
import argparse
import paho.mqtt.client as mqtt

def load_profiles(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Error loading profiles: {str(e)}\n")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='MQTT Pipe Sender')
    parser.add_argument('topic', help='MQTT topic to publish to')
    parser.add_argument('profiles_file', help='JSON file containing MQTT profiles')
    parser.add_argument('profile_name', help='Profile name to use from profiles file')
    args = parser.parse_args()

    profiles = load_profiles(args.profiles_file)
    profile = profiles.get(args.profile_name)

    if not profile:
        sys.stderr.write(f"Profile '{args.profile_name}' not found\n")
        sys.exit(1)

    # Set default keepalive if missing
    keepalive = profile.get('keepalive', 60)

    client = mqtt.Client()
    if 'username' in profile and 'password' in profile:
        client.username_pw_set(profile['username'].strip(), profile['password'].strip())

    try:
        client.connect(profile['host'], int(profile['port']), keepalive)
        client.loop_start()
        
        line_count = 0
        for line in sys.stdin:
            message = line.rstrip('\n')
            result = client.publish(args.topic, message, qos=1)
            result.wait_for_publish()  # Ensure message is delivered
            line_count += 1
        
        client.loop_stop()
        client.disconnect()
        sys.stderr.write(f"Sent {line_count} messages\n")
    except Exception as e:
        sys.stderr.write(f"MQTT error: {str(e)}\n")
        sys.exit(1)

if __name__ == '__main__':
    main()
