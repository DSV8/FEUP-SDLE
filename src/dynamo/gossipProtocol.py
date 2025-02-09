import orjson, zlib, jsonpickle, zmq, time, threading

class GossipProtocol:
    def __init__(self, node_id, node, known_nodes=None):
        self.node_id = node_id
        self.node = node  # Reference to the Error gossiping to update the hash ring
        self.node_states = {}  # Node states for each known node (alive or dead)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)  # Request socket
        self.shutdown_flag = False
        self.known_nodes = known_nodes

    def gossip(self):
        while not self.shutdown_flag:
            for node in self.known_nodes:
                try:
                    self.socket.connect(f"{node['address']}")  # Open the connection once for each gossip cycle
                    self.socket.send(self.compress_data({
                        "operation": "gossip",
                        "node_id": self.node_id,
                        "node_states": self.node_states,
                        "hash_ring": self.node.hash_ring.ring  # Gossip the current hash ring
                    })
                    )
                    response = self.decompress_data(self.socket.recv())
                    # Check the status of the response
                    if response.get("status") == "success":
                        # Optionally, merge the node states and hash ring if necessary
                        if "node_states" in response:
                            self.merge_states(response["node_states"])
                        else:
                            self.node_states[node['node_id']] = "dead"
                    else:
                        print(f"Node {self.node_id}: Failed to process gossip from {node['node_id']}")
                except Exception as e:
                    print(f"Error gossiping with {node['node_id']}: {e}")
                    self.node_states[node['node_id']] = "dead"
            time.sleep(10)  # Gossip every 10 seconds

    def merge_states(self, remote_states):
        """Merge the states of remote nodes with the local state."""
        for node, state in remote_states.items():
            if state == "dead" and self.node_states.get(node, "alive") != "dead":
                print(f"Node {node} is now marked as dead")
                self.node_states[node] = "dead"
                self.node.update_hash_ring(node, "dead")  # Remove from hash ring
            elif state == "alive" and self.node_states.get(node, "dead") == "dead":
                print(f"Node {node} is now marked as alive")
                self.node_states[node] = "alive"
                self.node.update_hash_ring(node, "alive")  # Add to hash ring

    def start(self):
        """Start the gossiping process in a separate thread."""
        threading.Thread(target=self.gossip, daemon=True).start()

    def stop(self):
        """Stop the gossiping thread."""
        self.shutdown_flag = True

    def compress_data(self, data):
        """
        Compress JSON data to be sent over ZMQ.
        :param data: The data (dict) to compress.
        :return: Compressed byte data
        """
        # Convert all dictionary keys to strings to avoid JSON serialization error
        if 'hash_ring' in data:
            # Convert keys in the 'hash_ring' dictionary to strings
            data['hash_ring'] = {str(key): value for key, value in data['hash_ring'].items()}

        if 'shopping_list' in data:
            # Use jsonpickle to serialize the custom object
            data['shopping_list'] = jsonpickle.dumps(data['shopping_list'])
        
        byte_data = orjson.dumps(data) # Convert the entire dictionary to JSON string
    
        # Compress the string into bytes
        compressed_data = zlib.compress(byte_data)  # Ensure it's in byte form
        return compressed_data      

    def decompress_data(self, compressed_data):
        """
        Decompress the data and convert it back to the original format.
        :param compressed_data: The compressed data received.
        :return: Decompressed and converted data
        """
        # Decompress the data first
        json_data = zlib.decompress(compressed_data)  # Decode from bytes to string

        # Load the JSON string into a Python dictionary
        data = orjson.loads(json_data)

        # Convert keys of the 'hash_ring' back to integers
        if 'hash_ring' in data:
            data['hash_ring'] = {int(key): value for key, value in data['hash_ring'].items()}

        # Decompress list object
        if 'shopping_list' in data:
            data['shopping_list'] = jsonpickle.loads(data['shopping_list'])

        return data

