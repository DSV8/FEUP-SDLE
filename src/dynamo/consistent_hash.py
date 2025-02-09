import hashlib
import bisect

class ConsistentHash:
    def __init__(self, replicas=3, hash_bits=32):
        """
        :param replicas: Number of virtual nodes for each physical node
        :param hash_bits: Number of bits to use for the reduced hash
        """
        self.replicas = replicas
        self.hash_mask = (1 << hash_bits) - 1  # Create a mask for the desired hash size
        self.ring = {}            # The hash ring
        self.sorted_keys = []     # Sorted keys for binary search
        self.nodes = {}           # Track physical nodes

    def _hash(self, key):
        """
        Hash function using SHA-256 and bitmasking to reduce hash size.
        :param key: Input key to hash
        :return: Reduced hash value
        """
        full_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return full_hash & self.hash_mask  # Apply bitmask to reduce hash size

    def add_node(self, node):
        """Add a physical node and its virtual replicas to the ring."""
        self.nodes[node] = f"tcp://127.0.0.1:{5000 + int(node[-1])}"  # Assign an address based on node id
        for i in range(self.replicas):
            replica_key = f"{node}-{i}"
            hash_key = self._hash(replica_key)
            self.ring[hash_key] = node
            bisect.insort(self.sorted_keys, hash_key)

    def remove_node(self, node):
        """Remove a physical node and its virtual replicas from the ring."""
        self.nodes.pop(node, None)  # Remove the node's address from the nodes map
        for i in range(self.replicas):
            replica_key = f"{node}-{i}"
            hash_key = self._hash(replica_key)
            self.ring.pop(hash_key, None)
            self.sorted_keys.remove(hash_key)

    def get_node(self, key):
        """Get the closest node for the given key."""
        hash_key = self._hash(key)
        index = bisect.bisect(self.sorted_keys, hash_key) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[index]]

    def get_nodes(self):
        """Returns a list of all physical nodes in the hash ring."""
        return list(self.nodes.keys())
