import zmq, time, sys
from threading import Thread
from dynamo.consistent_hash import ConsistentHash
from dynamo.replication_manager import ReplicationManager
from dynamo.node import Node
from storage.shopping_list_manager import ShoppingListManager

def start_node(node_id, port, hash_ring, replication_manager, known_nodes):
    """
    Start a Node instance as a separate process.
    :param node_id: Unique identifier for the node.
    :param port: The port the node will bind to.
    :param hash_ring: The consistent hash ring instance.
    :param replication_manager: The replication manager instance.
    """
    node = Node(node_id=node_id, port=port, hash_ring=hash_ring,
                replication_manager=replication_manager, known_nodes=known_nodes)
    node.start()

def start_proxy(context):
    # Create a REQ socket for the server-side (clients request here)
    frontend = context.socket(zmq.ROUTER)  # This is for client requests
    frontend.bind("tcp://*:5558")

    backend = context.socket(zmq.ROUTER)  # This is for server requests
    backend.setsockopt(zmq.IDENTITY, b"proxy_identity")
    backend.setsockopt(zmq.ROUTER_MANDATORY, 1)
    backend.bind("tcp://*:5559")

    print("Proxy started with ROUTER-DEALER pattern")

    poller = zmq.Poller()

    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    return frontend, backend, poller

def run_server():
    # Initialize the Hash Ring
    hash_ring = ConsistentHash()

    # Initialize the Shopping List Manager for compression and decompression
    manager = ShoppingListManager()

    # Define nodes with their IDs and ports
    nodes_config = [
        {"node_id": "node1", "port": 5001, "address": "tcp://localhost:5001"},
        {"node_id": "node2", "port": 5002, "address": "tcp://localhost:5002"},
        {"node_id": "node3", "port": 5003, "address": "tcp://localhost:5003"},
        {"node_id": "node4", "port": 5004, "address": "tcp://localhost:5004"},
        {"node_id": "node5", "port": 5005, "address": "tcp://localhost:5005"}
    ]

    context = zmq.Context()

    # Add nodes to the hash ring
    for config in nodes_config:
        hash_ring.add_node(config["node_id"])

    nodes_dict = {node["node_id"]: node["address"] for node in nodes_config}
    # Initialize ReplicationManager
    replication_manager = ReplicationManager(hash_ring, replication_factor=3, nodes_config=nodes_dict)

    # Start Node Threads
    threads = []
    for config in nodes_config:
        thread = Thread(target=start_node, args=(config["node_id"], config["port"], hash_ring, replication_manager, nodes_config))
        thread.start()
        threads.append(thread)
        print(f"Started {config['node_id']} on port {config['port']}")
        time.sleep(1)

    print("Await all nodes to start...")
    time.sleep(1)

    # Start Proxy
    frontend, backend, poller = start_proxy(context)

    try:
        # Handle requests from clients
        while True:
            sockets = dict(poller.poll(100))

            if frontend in sockets and sockets[frontend] == zmq.POLLIN:
                # Receive client request
                client_id, _, compressed_message = frontend.recv_multipart()
                message = manager.decompress_data(compressed_message)
                print(f"Proxy received: {message}")

                if(message["operation"] == "ping"):
                    # Respond to the ping request
                    frontend.send_multipart([client_id, b'', b"pong"])
                    continue

                # Use the hash ring to find the appropriate node for the request
                key = message["list_id"]

                responsible_node = hash_ring.get_node(key)

                print(f"Proxy forwarding request to node {responsible_node} for key={key}")

                backend.send_multipart([responsible_node.encode(), b'', client_id, compressed_message])

            if backend in sockets and sockets[backend] == zmq.POLLIN:
                # Wait for the response from the node and send it back to the client
                _, _, client_id, _, response = backend.recv_multipart()
                frontend.send_multipart([client_id, b'', response])

            time.sleep(0.1) # Sleep for a while to prevent high CPU usage

    except KeyboardInterrupt:
        print("\nShutting down all nodes...")
        for thread in threads:
            thread.join()

        frontend.close()
        backend.close()
        context.term()

if __name__ == "__main__":
    run_server()
