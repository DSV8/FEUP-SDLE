import uuid
from .or_map import ORMap

class ShoppingList:
    def __init__(self):
        # OR-Map
        self.or_map = ORMap()

    # Add item to shopping list
    def add_item(self, item_name, quantity=1):
        item_id = str(uuid.uuid4())
        self.or_map.add(item_id, item_name)
        self.or_map.increment_quantity(item_id, quantity)

    # Remove item from shopping list
    def remove_item(self, item_id):
        self.or_map.remove(item_id)

    # Mark item from shopping list as acquired
    def mark_item_acquired(self, item_id):
        self.or_map.mark_as_acquired(item_id)

    # Increment quantity of item
    def increment_quantity(self, item_id, value=1):
        self.or_map.increment_quantity(item_id, value)

    # Decrement quantity of item
    def decrement_quantity(self, item_id, value=1):
        self.or_map.decrement_quantity(item_id, value)

    # Get items not yet acquired or removed
    def get_shopping_list(self):
        return self.or_map.get_items()
    
    # Get all items
    def get_all_items(self):
        return self.or_map.get_all_items()

    # Merge two shopping list instances
    def merge(self, other):
        self.or_map.merge(other.or_map)

    # Print list's contents and their quantities
    def display_list(self):
        items = self.get_shopping_list()
        if not items:
            print("\nThe shopping list is empty.")
        else:
            print("\nThese are the products currently in your list:")
            for item_id, (item_name, quantity, _) in items.items():
                item_name_cap = item_name.capitalize()
                print(f"- {item_name_cap}: [x{quantity}]")