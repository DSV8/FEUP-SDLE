import zmq, orjson, jsonpickle, zlib

class ReplicationManager:
    def __init__(self, hash_ring, replication_factor=3, nodes_config=None):
        """
        Handles replication of data across nodes.
        :param hash_ring: Instance of ConsistentHash.
        :param replication_factor: Number of replicas for each key.
        :param nodes_config: Configuration of nodes with their node_id and addresses.
        """
        self.context = zmq.Context()
        self.hash_ring = hash_ring
        self.replication_factor = replication_factor
        self.nodes_config = nodes_config

    def get_replicas(self, key):
        """
        Get the replica nodes for a given key.
        :param key: The key to locate in the hash ring.
        :return: List of nodes responsible for the key.
        """
        primary_node = self.hash_ring.get_node(key)
        all_nodes = self.hash_ring.get_nodes()
        start_index = all_nodes.index(primary_node)
        
        # Get the replica nodes
        return [all_nodes[(start_index + i) % len(all_nodes)] for i in range(self.replication_factor)]

    def replicate_to_node(self, node_id, list_id, list):
        """
        Send a write request to a node.
        :param node_id: The node ID to replicate data to.
        :param list_id: The list_id to replicate.
        :param value: The list state to replicate.
        :return: True if the replication was successful, False otherwise.
        """
        try:
            # Get the node socket using the node_id
            address = self.nodes_config[node_id]
            if not address:
                print(f"Node address for {node_id} not found.")
                return False
            
            socket = self.context.socket(zmq.REQ)
            socket.connect(address)

            # Print the node address and key before trying to connect
            print(f"Attempting to replicate to node {node_id} for key={list_id}")

            message = {
                "operation": "replicate",
                "list_id": list_id,
                "shopping_list": list
            }
        
            # Compress the data before sending
            compressed_message = self.compress_data(message)

            # Send the write request
            socket.send(compressed_message)

            print(f"Sent write request to node {node_id} for key={list_id}")

            # Receive acknowledgment
            ack = socket.recv()

            # Decompress the response
            ack = self.decompress_data(ack)
            print(f"Replication to node {node_id} completed with response: {ack}")

            socket.close()
        
            return ack['status'] == 'success'

        except zmq.ZMQError as e:
            print(f"ZeroMQ error occurred while replicating to {node_id}: {e}")
            return False
        except Exception as e:
            print(f"Failed to replicate to {node_id}: {e}")
            return False
        
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