class ORSet:
    def __init__(self):
        self.add_set = set()
        self.remove_set = set()
    
    # Add item
    def add(self, item_id):
        self.add_set.add(item_id)
    
    # Remove item
    def remove(self, item_id):
        if item_id in self.add_set:
            self.remove_set.add(item_id)
    
    # Get items that are in add_set but not in remove_set
    def get_items(self):
        return self.add_set.difference(self.remove_set)
    
    # Merge with another OR-Set instance
    def merge(self, other):
        self.add_set = self.add_set.union(other.add_set)
        self.remove_set = self.remove_set.union(other.remove_set)

    def get_remove_set(self):
        return self.remove_set