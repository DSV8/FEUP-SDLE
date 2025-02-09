import zmq, uuid
from storage.shopping_list_manager import ShoppingListManager

class Client:
    # Initialize client with REQ-REP proxy address
    def __init__(self, proxy_req_address="tcp://localhost:5558"):
        self.context = zmq.Context.instance()
        # REQ-REP socket for receiving updates
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(proxy_req_address)

        self.shopping_list_manager = ShoppingListManager()

        self.server_availabilty = False


    def check_server_availability(self):
        """Ping the server to check availability."""
        ping_socket = self.context.socket(zmq.REQ)
        ping_socket.RCVTIMEO = 1000
        ping_socket.connect("tcp://localhost:5558")  # Replace with actual address

        availability = False
        try:
            message = {"operation": "ping"}
            compressed_message = self.shopping_list_manager.compress_data(message)
            ping_socket.send(compressed_message)
            print("Pinging server...")
            ping_socket.recv()  # Expect a response to the ping
            availability = True
        except zmq.error.Again:
            availability = False
        finally:
            ping_socket.close()
            return availability

    def send_request(self, operation, payload=None, list=None):
        request = {"operation": operation}
        if payload != None:
            request.update(payload)
        if list != None:
            request["shopping_list"] = list

        if not self.server_availabilty:
            self.server_availabilty = self.check_server_availability()
        if not self.server_availabilty:
            print("Server is not available")
            return request

        print(f"\nSending request: {request}")
            
        self.req_socket.send(self.shopping_list_manager.compress_data(request))
        response = self.shopping_list_manager.decompress_data(self.req_socket.recv())
        return response

    # Create new shopping list
    def create_shopping_list(self, list_id):
        payload = {"list_id": list_id}
        response = self.send_request("create", payload)
        print(f"Shopping list created with ID: {response['list_id']}")

    # Fetch shopping list by ID
    def get_shopping_list(self, list_id):
        payload = {"list_id": list_id}
        response = self.send_request("read", payload)
        if "shopping_list" in response:
            return response['shopping_list']
        elif self.server_availabilty == True:
            return False
        elif self.server_availabilty == False:
            return True

    # Send list to node to be merged
    def write_shopping_list(self, list, list_id):
        payload = {"list_id": list_id}
        response = self.send_request("write", payload, list)
        if "shopping_list" in response:
            return response['shopping_list']
        return False

    # Delete shopping list by ID
    def delete_shopping_list(self, list_id):
        payload = {"list_id": list_id}
        response = self.send_request("delete", payload)
        print(f"Shopping list with ID {response['list_id']} deleted.")


    def close_all_sockets(self):
        self.req_socket.close()
        print("Closed all sockets and terminated context.")
