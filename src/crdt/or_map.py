import uuid
from .pn_counter import PNCounter

class ORMap:
    def __init__(self):
        # Add set: {item_id: (item_name, PNCounter, acquired_flag)}
        self.add_map = {}
        # Remove set: {item_id: (item_name, PNCounter, acquired_flag)}
        self.removed_map = {}
        # Acquired set: {item_id: (item_name, PNCounter, acquired_flag)}
        self.acquired_map = {}

    # Add item with unique ID, name and initialize acquired flag
    def add(self, item_id, item_name):
        if item_id not in self.add_map:
            self.add_map[item_id] = (item_name, PNCounter(), False)

    # Logically remove item
    def remove(self, item_id):
        if ((item_id in self.add_map) and (item_id not in self.acquired_map)):
            item_name, counter, acquired = self.add_map[item_id]
            # Add item to remove set
            self.removed_map[item_id] = (item_name, counter, acquired)
            # Set counters to zero in the remove set
            self.removed_map[item_id][1].positive = 0
            self.removed_map[item_id][1].negative = 0

    # Mark item as acquired
    def mark_as_acquired(self, item_id):
        if ((item_id in self.add_map) and (item_id not in self.removed_map)):
            item_name, counter, _ = self.add_map[item_id]

            #del self.add_map[item_id]
            self.acquired_map[item_id] = (item_name, counter, True)
            self.add_map[item_id] = (item_name, counter, True)

    # Increment quantity of item
    def increment_quantity(self, item_id, value):
        if item_id in self.add_map:
            _, counter, _ = self.add_map[item_id]
            counter.increment(value)

    # Decrement quantity of item
    def decrement_quantity(self, item_id, value):
        if item_id in self.add_map:
            item_name, counter, acquired = self.add_map[item_id]
            counter.decrement(value)
            if counter.get_count() <= 0:
                self.remove(item_id)

    # Retrieve effective items with their effective count
    def get_items(self):
        removed_or_acquired = set(self.removed_map.keys()).union(self.acquired_map.keys())
        resolved_items = {
            item_id: (item_name, counter.get_count(), acquired)
            for item_id, (item_name, counter, acquired) in self.add_map.items()
            if item_id not in removed_or_acquired
        }
        return resolved_items

     # Retrieve removed items
    def get_removed_items(self):
        acquired_items_ids = set(self.acquired_map.keys())
        resolved_items = {
            item_id: (item_name, counter, acquired)
            for item_id, (item_name, counter, acquired) in self.removed_map.items()
            if item_id not in acquired_items_ids
        }
        return resolved_items

    # Retrieve acquired items
    def get_acquired_items(self):
        removed_item_ids = set(self.removed_map.keys())
        resolved_items = {
            item_id: (item_name, counter, acquired)
            for item_id, (item_name, counter, acquired) in self.acquired_map.items()
            if item_id not in removed_item_ids
        }
        return

    # Retrieve all items
    def get_all_items(self):
        return {
            item_id: (item_name, counter.get_count(), acquired)
            for item_id, (item_name, counter, acquired) in self.add_map.items()
        }

    # Merge CRDTs
    def merge(self, other):
        # Temporary map to store merged items by name
        merged_items = {}

        # Iterate over the items in the other's add_map
        for item_id, (item_name, other_counter, acquired) in other.add_map.items():
            # Skip items that are in removed_map or acquired_map
            if item_id in self.removed_map or item_id in self.acquired_map or item_id in other.removed_map or item_id in other.acquired_map:
                continue

            # Check if an item with the same name exists in self's add_map
            existing_item_id = None
            for self_item_id, (self_item_name, self_counter, self_acquired) in self.add_map.items():
                if self_item_name == item_name:
                    existing_item_id = self_item_id
                    break

            if existing_item_id:
                # Merge counters and remove the existing item from add_map
                _, self_counter, _ = self.add_map.pop(existing_item_id)
                other_counter.merge_max(self_counter)
                merged_items[item_name] = other_counter
            else:
                # No matching item name in self, add directly
                if item_name not in merged_items:
                    merged_items[item_name] = other_counter

        # Add all merged items back to add_map with new item_ids
        for item_name, merged_counter in merged_items.items():
            new_item_id = str(uuid.uuid4())
            self.add_map[new_item_id] = (item_name, merged_counter, False)

        # Merge removed_map entries
        for item_id, (item_name, other_counter, acquired) in other.removed_map.items():
            if item_id not in self.removed_map:
                self.removed_map[item_id] = (item_name, other_counter, acquired)
                self.removed_map[item_id][1].positive = 0
                self.removed_map[item_id][1].negative = 0

        # Merge all entries from removed_map to add_map
        for item_id, (item_name, counter, acquired) in self.removed_map.items():
            self.add_map[item_id] = (item_name, counter, acquired)

        # Merge acquired_map entries
        for item_id, (item_name, other_counter, acquired) in other.acquired_map.items():
            if item_id not in self.acquired_map:
                self.acquired_map[item_id] = (item_name, other_counter, acquired)
            self.acquired_map[item_id][1].merge_max(other_counter)

        # Merge all entries from acquired_map to add_map
        for item_id, (item_name, counter, acquired) in self.acquired_map.items():
            self.add_map[item_id] = (item_name, counter, acquired)
