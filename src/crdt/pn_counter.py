class PNCounter:
    def __init__(self):
        self.positive = 0
        self.negative = 0
    
    # Increment positive counter
    def increment(self, value=1):
        self.positive += value
    
    # Decrement (Increment negative counter)
    def decrement(self, value=1): 
        self.negative += value
    
    # Get current count
    def get_count(self):
        return self.positive - self.negative
    
    # Merge with another PN-Counter
    def merge(self, other):
        self.positive += other.positive
        self.negative += other.negative

    # Merge with another PN-Counter (different approach)
    def merge_max(self, other):
        self.positive = max(self.positive, other.positive)
        self.negative = max(self.negative, other.negative)