import orjson, uuid, zlib, jsonpickle
from crdt.shopping_list import ShoppingList
from crdt.pn_counter import PNCounter
from crdt.or_set import ORSet

# Paths to JSON database
DATA_PATH = 'data/shopping_list_data.json'

class ShoppingListManager:
    def __init__(self):
        # Dictionary to store shopping lists by their unique IDs
        self.shopping_lists = {}
        # Set to control which lists are currently still active (not deleted by the user)
        self.list_ids = ORSet()

    # Create new shopping list with unique ID
    def create_shopping_list(self):
        list_id = str(uuid.uuid4())
        self.shopping_lists[list_id] = ShoppingList()
        self.list_ids.add(list_id)
        return list_id
    
    # Create shopping list with existing ID
    def create_shopping_list_with_id(self, list_id):
        self.shopping_lists[list_id] = ShoppingList()
        self.list_ids.add(list_id)
        return list_id

    # Delete specific shopping list by ID
    def delete_shopping_list(self, list_id):
        if list_id in self.shopping_lists:
            del self.shopping_lists[list_id]
            self.list_ids.remove(list_id)
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")
    
    # Add product to shopping list with specific ID
    def add_item_to_list(self, list_id, item_name, quantity=1):
        item_name_cap = item_name.capitalize()

        if list_id in self.shopping_lists:
            shopping_list = self.shopping_lists[list_id]
            items = shopping_list.get_shopping_list()
            for _, (name, _, _) in items.items():
                if name == item_name:
                    print(f"\n\033[31;1mError:\033[0m Item '{item_name_cap}' already exists in the shopping list.")
                    return
            self.shopping_lists[list_id].add_item(item_name, quantity)
            print(f"\n{item_name_cap} was added to your shopping list successfully!")
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")

    # Remove product from shopping list with specific ID
    def remove_item_from_list(self, list_id, item_id):
        if list_id in self.shopping_lists:
            self.shopping_lists[list_id].remove_item(item_id)
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")
        
    # Acquire product from shopping list with specific ID
    def acquire_item_from_list(self, list_id, item_id):
        if list_id in self.shopping_lists:
            self.shopping_lists[list_id].mark_item_acquired(item_id)
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")

    # Increment quantity of product of shopping list with specific ID
    def increment_product_quantity(self, list_id, item_id, value=1):
        if list_id in self.shopping_lists:
            shopping_list = self.shopping_lists[list_id]
            items = shopping_list.get_shopping_list()
            if item_id in items:
                shopping_list.increment_quantity(item_id, value)
            else:
                print(f"\nProduct with ID {item_id} does not exist in list {list_id}.")
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")
        
    # Decrement quantity of product of shopping list with specific ID
    def decrement_product_quantity(self, list_id, item_id, value=1):
        if list_id in self.shopping_lists:
            shopping_list = self.shopping_lists[list_id]
            items = shopping_list.get_shopping_list()
            if item_id in items:
                shopping_list.decrement_quantity(item_id, value)
            else:
                print(f"\nProduct with ID {item_id} does not exist in list {list_id}.")
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")

    # Retrieve specific shopping list by ID
    def get_shopping_list(self, list_id):
        if list_id in self.shopping_lists:
            return self.shopping_lists[list_id]
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")

    # Get list IDs of lists still active (that were not deleted by the user)
    def get_lists_still_active(self):
        return self.list_ids.get_items()
    
    # Get deleted lists
    def get_removed_lists(self):
        return self.list_ids.get_remove_set()
        
    # Get item ID through item name of specific list
    def get_item_id_by_name(self, list_id, item_name):
        if list_id in self.shopping_lists:
            shopping_list = self.shopping_lists[list_id]
            items = shopping_list.get_shopping_list()
            for item_id, (name, _, _) in items.items():
                if name == item_name:
                    return item_id
            return
        else:
            print(f"\nShopping list with ID {list_id} does not exist in your local environment.")


    # Save all shopping lists to JSON file
    def save_to_json(self):
        data = {}
        for list_id, shopping_list in self.shopping_lists.items():
            data[list_id] = {
                "add_map": {
                    item_id: {
                        "name": item_name,
                        "pn_counter": {
                            "positive": counter.positive,
                            "negative": counter.negative
                        },
                        "acquired": acquired
                    }
                    for item_id, (item_name, counter, acquired) in shopping_list.or_map.add_map.items()
                },
                "removed_map": {
                    item_id: {
                        "name": item_name,
                        "pn_counter": {
                            "positive": counter.positive,
                            "negative": counter.negative
                        },
                        "acquired": acquired
                    }
                    for item_id, (item_name, counter, acquired) in shopping_list.or_map.removed_map.items()
                },
                "acquired_map": {
                    item_id: {
                        "name": item_name,
                        "pn_counter": {
                            "positive": counter.positive,
                            "negative": counter.negative
                        },
                        "acquired": acquired
                    }
                    for item_id, (item_name, counter, acquired) in shopping_list.or_map.acquired_map.items()
                }
            }
        # Serialize using orjson
        with open(DATA_PATH, 'wb') as file:
            file.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    # Load all shopping lists from JSON file
    def load_from_json(self):
        try:
            with open(DATA_PATH, 'rb') as file:
                data = orjson.loads(file.read())
        except FileNotFoundError:
            data = {}

        self.shopping_lists = {}
        for list_id, shopping_list_data in data.items():
            shopping_list = ShoppingList()

            # Rebuild add_map
            for item_id, item_data in shopping_list_data.get("add_map", {}).items():
                item_name = item_data["name"]
                pn_data = item_data["pn_counter"]
                pn_counter = PNCounter()
                pn_counter.positive = pn_data["positive"]
                pn_counter.negative = pn_data["negative"]
                acquired = item_data["acquired"]
                shopping_list.or_map.add_map[item_id] = (item_name, pn_counter, acquired)

            # Rebuild removed_map
            for item_id, item_data in shopping_list_data.get("removed_map", {}).items():
                item_name = item_data["name"]
                pn_data = item_data["pn_counter"]
                pn_counter = PNCounter()
                pn_counter.positive = pn_data["positive"]
                pn_counter.negative = pn_data["negative"]
                acquired = item_data["acquired"]
                shopping_list.or_map.removed_map[item_id] = (item_name, pn_counter, acquired)

            # Rebuild acquired_map
            for item_id, item_data in shopping_list_data.get("acquired_map", {}).items():
                item_name = item_data["name"]
                pn_data = item_data["pn_counter"]
                pn_counter = PNCounter()
                pn_counter.positive = pn_data["positive"]
                pn_counter.negative = pn_data["negative"]
                acquired = item_data["acquired"]
                shopping_list.or_map.acquired_map[item_id] = (item_name, pn_counter, acquired)

            self.shopping_lists[list_id] = shopping_list

    # Compress JSON data to be sent over ZMQ
    def compress_data(self, data):
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

    # Decompress data and convert it back to original format
    def decompress_data(self, compressed_data):
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
