import zmq, threading
from .gossipProtocol import GossipProtocol
from .consistent_hash import ConsistentHash
from storage.shopping_list_manager import ShoppingListManager

class Node:
    def __init__(self, node_id, port, hash_ring=None, replication_manager=None, known_nodes=None):
        self.node_id = node_id
        self.port = port
        self.hash_ring = hash_ring  # Reference to the consistent hash ring
        self.replication_manager = replication_manager  # Reference to the replication manager
        self.context = zmq.Context()

        self.rep_socket = self.context.socket(zmq.REP)
        self.rep_socket.bind(f"tcp://*:{self.port}")
        print(f"Node {self.node_id}: Listening for requests on tcp://*:{self.port}")

        self.dealer_socket = self.context.socket(zmq.DEALER)
        
        # Set the dealer socket identity
        self.dealer_socket.setsockopt(zmq.IDENTITY, node_id.encode())
        self.dealer_socket.connect("tcp://localhost:5559")
        print(f"{self.dealer_socket.identity} connected to tcp://localhost:5559")
        
        # start poller
        self.poller = zmq.Poller()
        self.poller.register(self.rep_socket, zmq.POLLIN)
        self.poller.register(self.dealer_socket, zmq.POLLIN)

        # Initialize Gossip Protocol
        self.gossip_protocol = GossipProtocol(self.node_id, self, known_nodes)
        self.gossip_protocol.start()  # Start gossiping in a separate thread

        # Initialize ShoppingListManager
        self.shopping_manager = ShoppingListManager()

    # Handles messages received from proxy
    def handle_message(self, topic, message):
        if topic != "gossip":    
            print(f"Node {self.node_id}: Handling message with topic={topic}, message={message}")

        # Handle write operation
        if topic == "write":
            try:
                return self.handle_write(message)
            except KeyError as e:
                return {"error": str(e)}

        # Handle read operation
        elif topic == "read":
            try:
                return self.handle_read(message)
            except KeyError as e:
                return {"error": str(e)}
        
        # Handle list creation
        elif topic == "create":
            try:
                return self.handle_creation(message)
            except KeyError as e:
                return {"error": str(e)}
            
        # Handle list deletion
        elif topic == "delete":
            try:
                return self.handle_deletion(message)
            except KeyError as e:
                return {"error": str(e)}
            
        # Handle replication operation
        elif topic == "replicate":    
            try:
                ack = self.handle_replicate(message)
                if ack == "success":
                    return {"status": "success"}   
                else:
                    return {"status": "error"}
            except Exception as e:
                return {"status": "error", "message": str(e)}         
        
        # Handle gossip operation
        elif topic == "gossip":
            # Process the gossip message
            remote_node_id = message["node_id"]
            remote_node_states = message["node_states"]
            remote_hash_ring = message["hash_ring"]

            # Merge the gossip states
            self.gossip_protocol.merge_states(remote_node_states)

            # Update the local hash ring based on the remote hash ring
            self.merge_hash_ring(remote_hash_ring)

            # Send a response acknowledging the gossip
            response = {
                "status": "success",  # Acknowledge success
                "message": f"Gossip processed from node {remote_node_id}",  # Optional detailed message
                "node_id": self.node_id,  # Include the local node's ID
                "node_states": self.gossip_protocol.node_states,  # Optionally, include the current local node states
                "hash_ring": self.hash_ring.ring  # Optionally, include the updated hash ring
            }

            return response
        
        # Handle unknown operation
        else:
            print(f"Node {self.node_id}: Unknown topic {topic}")
    
    def handle_write(self, message):
        """
        Handles the write operation.
        Merges the incoming list with the local state.
        """
        list = message["shopping_list"]
        list_id = message['list_id']

        print(f"Node {self.node_id}: Write operation for key={list_id} with list={list}")

        if list_id in self.shopping_manager.get_removed_lists():
            raise KeyError(f"Shopping list with ID {list_id} has been deleted.")

        if list_id not in self.shopping_manager.get_lists_still_active():
            self.shopping_manager.create_shopping_list_with_id(list_id)

        # Merge the shopping lists with the same item_id 
        self.shopping_manager.shopping_lists[list_id].merge(list)

        print(f"Node {self.node_id}: Write operation completed for key={list_id}")
    
        return {'shopping_list': self.shopping_manager.shopping_lists[list_id] }  # Send acknowledgment for write operation

    def handle_read(self, message):
        """
        Handles the read operation.
        Returns the current state of the requested shopping list.
        """
        list_id = message["list_id"]
        try:
            list = self.shopping_manager.get_shopping_list(list_id)
            if(list == None):
                raise KeyError(f"Shopping list with ID {list_id} does not exist.")
        except KeyError as e:
            raise KeyError(f"Shopping list with ID {list_id} does not exist.")

        print(f"Node {self.node_id}: Read operation completed. List: {list}")
        return {'shopping_list': list }  # Return current shopping list items
    
    # Create new shopping list
    def handle_creation(self, message):
        list_id = message["list_id"]

        # Check if the list already exists
        if list_id in self.shopping_manager.shopping_lists:
            raise KeyError(f"Shopping list with ID {list_id} already exists.")
        
        # Create a new empty shopping list and add to set
        self.shopping_manager.create_shopping_list_with_id(list_id)
        print(f"Node {self.node_id}: Created new shopping list with ID {list_id}")

        return {"list_id": list_id}
    
    def handle_deletion(self, message):
        self.shopping_manager.delete_shopping_list(message["list_id"])
        return {"list_id": message["list_id"]}

    # Handle replication
    def handle_replicate(self, message):
        try:
            list_id = message["list_id"]
            list = message["shopping_list"]

            # if the list id is not found, it means we're replicating a newly created list
            if list_id not in self.shopping_manager.shopping_lists:
                self.shopping_manager.create_shopping_list_with_id(list_id)

            if(list == None):
                # If the list is empty, it means we're replicating a deletion
                self.shopping_manager.delete_shopping_list(list_id)
            else:
                # Merge the shopping lists with the same item_id
                self.shopping_manager.shopping_lists[list_id].merge(list)
        
            print(f"Node {self.node_id}: Replication completed for list_id={list_id}")
            return "success"
        except Exception as e:
            print(f"Node {self.node_id}: Error replicating data: {e}")
            return "error"

    # Add new node to hash ring
    def add_node(self, new_node_id):
        print(f"Node {self.node_id}: Adding new node {new_node_id} to the hash ring")
        self.hash_ring.add_node(new_node_id)

    # Remove node from hash ring
    def remove_node(self, node_id_to_remove):
        print(f"Node {self.node_id}: Removing node {node_id_to_remove} from the hash ring")
        self.hash_ring.remove_node(node_id_to_remove)

    # Function to handle replication for a single replica
    def replicate_to_single_node(self, replica, list_id, data):
        self.replication_manager.replicate_to_node(replica, list_id, data)
        print(f"Node {self.node_id}: Replication to node {replica} completed for list_id={list_id}")

    def replicate_to_nodes(self, message):
        list_id = message["list_id"]

         # Replicate the update to replicas
        replicas = self.replication_manager.get_replicas(list_id)
        for replica in replicas:
            if replica != self.node_id:  # Avoid self-replication
                if list_id not in self.shopping_manager.shopping_lists:
                    data = None
                else:
                    data = self.shopping_manager.shopping_lists[list_id]

                # Start a new thread to replicate to the replica
                replication_thread = threading.Thread(target=self.replicate_to_single_node, args=(replica, list_id, data))
                replication_thread.start()

    # Start listening for messages (direct REQ-REP communication)
    def start(self):
        while True:
            # Handle direct REQ-REP requests
            sockets = dict(self.poller.poll(100))

            if self.rep_socket in sockets:
                # Handle replication
                message = self.rep_socket.recv()
                decompressed_message = self.shopping_manager.decompress_data(message)
                response = self.handle_message(decompressed_message["operation"], decompressed_message)
                compressed_response = self.shopping_manager.compress_data(response)
                self.rep_socket.send(compressed_response)

            if self.dealer_socket in sockets:

                _, client_id, compressed_message = self.dealer_socket.recv_multipart()  # Blocking until a request is received
                print(f"Node {self.node_id}: Received message from proxy")
                message = self.shopping_manager.decompress_data(compressed_message)  # Blocking until a request is received

                decompressed_response = self.handle_message(message["operation"], message)
                response = self.shopping_manager.compress_data(decompressed_response)
                self.dealer_socket.send_multipart([b'proxy_identity', client_id, b'', response])

                if(message['operation'] == 'write' or message['operation'] == 'delete'):
                    self.replicate_to_nodes(message)

    # Update hash ring based on gossip state of node
    def update_hash_ring(self, node_id, state):
        if state == "dead":
            self.hash_ring.remove_node(node_id)
        elif state == "alive":
            self.hash_ring.add_node(node_id)

    def merge_hash_ring(self, remote_ring):
        for hash_key, node_id in remote_ring.items():
            if hash_key not in self.hash_ring.ring:
                self.hash_ring.ring[hash_key] = node_id
                if node_id not in self.hash_ring.nodes:
                    self.hash_ring.nodes[node_id] = f"tcp://127.0.0.1:{5000 + int(node_id[-1])}"