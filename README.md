# MQTT Pipe Tools

Tools for piping data through MQTT brokers

## Installation
```bash
git clone https://github.com/yourusername/mqtt-pipe-tools.git
cd mqtt-pipe-tools
pip install -r requirements.txt
```

## Configuration
1. Create `profiles.json` using the template:
   ```bash
   cp profiles.example.json profiles.json
   ```
2. Edit profiles.json with your broker details

## Usage
### Sender (publish stdin)
```bash
echo "Hello" | ./mqtt_send.py my/topic profiles.json profile_name
```

### Listener (subscribe to stdout)
```bash
./mqtt_listen.py my/topic profiles.json profile_name
```

### Binary Data Example
```bash
# Send image
cat image.jpg | ./mqtt_send.py images/topic profiles.json test

# Receive image
./mqtt_listen.py images/topic profiles.json test > received.jpg
```

## Features
- Binary-safe data handling
- Profile-based configuration
- QoS 1 guaranteed delivery
- Clean shutdown on SIGINT/SIGTERM
