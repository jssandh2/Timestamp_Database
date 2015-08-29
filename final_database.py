__author__ = 'juspreetsandhu1'

# NOTES:
# 1) The solution has comments in order to explain most functions in a fair amount of detail.
# 2) The solution generously uses hash-maps.
# 3) Time Complexity:
#   - Assuming the existence of a good hash-function, SET, UNSET, BEGIN, GET, NUMEQUALTO all run in O(1) time.
#   - ROLLBACK is a slightly expensive function, which if called on a large transaction block, can take ~ O(n) time.
#       : The reason is simply that if the we call roll-back on one huge transaction, we deepcopy the whole hash-table
# 4) Space Complexity:
#   - As we have optimized our Computational Complexity, we have used a little more Space than a non-optimal time sol.
#   - With the assumption that every transaction-block is small (say m), then we take linear space for that block: O(m)
#   - However, given multiple nested transactions of size m_{i}, we have our Net Space Complexity to be:
#   - sum_{i = 1}^{i = |transactions|} m_{i} = m_{1} + m_{2} + .. + m_{|transactions|} ~ O(m^2)
#   - Therefore, per block, our Space Complexity is linear, but, the nested Space Complexity is Quadratic.

import copy     # Used for deepcopy
import fileinput      # Used for the main function
from sys import stdin
import sys


class finalDatabase():

    def __init__(self):
        self.main_hash_table = {"key": {}, "value": {}}
        self.list_begins = []
        self.num_set_unset_commands = 0
        self.committed = False
        self.hash_begin_to_main_hash_table = {}
        self.first_begin = False

    def setValue(self, key, value):
        if self.committed:
            print("NO TRANSACTION")
            return
        if key in self.main_hash_table["key"]:
            # First, update the value to the new value
            old_value = self.main_hash_table["key"][key]
            self.main_hash_table["key"][key] = value
            # First, remove the key from the list of keys corresponding to the old value
            old_list_keys = self.main_hash_table["value"][old_value]
            del old_list_keys[key]
            self.main_hash_table["value"][old_value] = old_list_keys
            if not self.main_hash_table["value"][old_value]:
                del self.main_hash_table["value"][old_value]
            else:
                self.main_hash_table["value"][old_value] = old_list_keys
            self.helpSetValueInValueHash(key, value)
        else:
            # First, create a new key -> value pair in the nested "key" hashmap
            self.main_hash_table["key"][key] = value
            self.helpSetValueInValueHash(key, value)
        self.num_set_unset_commands += 1

    def helpSetValueInValueHash(self, key, value):
        if value in self.main_hash_table["value"]:
            # Second, insert the key into the place with given value, as value already exists in the database
            new_list_keys = self.main_hash_table["value"][value]
            new_list_keys[key] = key
            self.main_hash_table["value"][value] = new_list_keys
        else:
            # Create a new value -> key list hashmap, and push it in, for the new value
            new_list_keys = {key: key}
            self.main_hash_table["value"][value] = new_list_keys

    def getValue(self, key):
        if key in self.main_hash_table["key"]:
            # The key exists in the database, so print it out
            print(self.main_hash_table["key"][key])
            return
        else:
            # A non-existent key has been referenced
            print("NULL")
            return

    def unSetValue(self, key):
        if self.committed:
            print("NO TRANSACTION")
            return
        if key in self.main_hash_table["key"]:
            old_value = self.main_hash_table["key"][key]
            # Update the number of set/unset commands
            self.num_set_unset_commands += 1
            # Remove the key-> value hash
            del self.main_hash_table["key"][key]
            old_list_keys = self.main_hash_table["value"][old_value]
            # Remove the value-> key hash
            del old_list_keys[key]
            if not old_list_keys:
                del self.main_hash_table["value"][old_value]
            else:
                self.main_hash_table["value"][old_value] = old_list_keys
        else:
            return

    def numEqualTo(self, value):
        if value in self.main_hash_table["value"]:
            # The given value has corresponding keys in the Database
            print(len(self.main_hash_table["value"][value]))
        else:
            # There does not exist a key in the Database, such that key = value
            print(0)

    def beginTransaction(self):
        # A begin command was issued after the current number of set/unset commands, so memorize it.
        if self.committed:
            return
        if len(self.list_begins) == 0 and self.num_set_unset_commands == 0:
            self.first_begin = True
            self.list_begins.append(self.num_set_unset_commands)
            copy_main_hash_table = copy.deepcopy(self.main_hash_table)      # Python only does references, do deepcopy!
            self.hash_begin_to_main_hash_table[self.num_set_unset_commands] = copy_main_hash_table
        else:
            # A new BEGIN block with memory changes prior has been created, store it in an updated hash
            copy_main_hash_table = copy.deepcopy(self.main_hash_table)  # Python only does references, do deepcopy!
            self.hash_begin_to_main_hash_table[self.num_set_unset_commands] = copy_main_hash_table
            self.list_begins.append(self.num_set_unset_commands)

    def commitTransactions(self):
        # Set self.committed == True, and this will freeze all set/unset/rollback operations.
        self.committed = True

    def rollBackTransaction(self):
        if self.committed:     # Frozen transactions being accessed
            print("NO TRANSACTION")
            return
        elif len(self.list_begins) == 0 or self.num_set_unset_commands == 0:    # Edge case
            print("NO TRANSACTION")
        else:
            if len(self.list_begins) == 1:
                copy_required_hash_table = copy.deepcopy(self.hash_begin_to_main_hash_table[self.list_begins[0]])
                self.main_hash_table = copy_required_hash_table
                self.num_set_unset_commands = self.list_begins[0]
                self.list_begins.pop()
            else:
                list_last_index = self.list_begins[len(self.list_begins) - 1]
                copy_required_hash_table = copy.deepcopy(self.hash_begin_to_main_hash_table[list_last_index])
                self.main_hash_table = copy_required_hash_table
                self.num_set_unset_commands = list_last_index
                self.list_begins.pop()


if __name__ == '__main__':
    final_database = finalDatabase()
    if len(sys.argv) > 1:
        for line in fileinput.input():
            command = line.split(" ")
            aCommand = command[0]
            if len(command) == 1:
                aCommand = (command[0].split("\n"))[0]
                aCommand = str(aCommand)
            if aCommand == "SET":
                value = (command[2].split("\n"))[0]
                value = str(value)
                final_database.setValue(command[1], value)
            elif aCommand == "UNSET":
                key = (command[1].split("\n"))[0]
                final_database.unSetValue(key)
            elif aCommand == "NUMEQUALTO":
                value = (command[1].split("\n"))[0]
                final_database.numEqualTo(value)
            elif aCommand == "GET":
                key = (command[1].split("\n"))[0]
                final_database.getValue(key)
            elif aCommand == "BEGIN":
                final_database.beginTransaction()
            elif aCommand == "COMMIT":
                final_database.commitTransactions()
            elif aCommand == "ROLLBACK":
                final_database.rollBackTransaction()
            else:
                del final_database
    else:
        line = stdin.readline()
        command_split = line.split(" ")
        aCommand = command_split[0]
        if len(command_split) == 1:
            aCommand = (command_split[0].split("\n"))[0]
        if aCommand != "END":
            while aCommand != "END":
                if aCommand == "SET":
                    value = (command_split[2].split("\n"))[0]
                    value = str(value)
                    final_database.setValue(command_split[1], value)
                elif aCommand == "UNSET":
                    key = (command_split[1].split("\n"))[0]
                    final_database.unSetValue(key)
                elif aCommand == "NUMEQUALTO":
                    value = (command_split[1].split("\n"))[0]
                    final_database.numEqualTo(value)
                elif aCommand == "GET":
                    key = (command_split[1].split("\n"))[0]
                    final_database.getValue(key)
                elif aCommand == "BEGIN":
                    final_database.beginTransaction()
                elif aCommand == "COMMIT":
                    final_database.commitTransactions()
                elif aCommand == "ROLLBACK":
                    final_database.rollBackTransaction()
                else:
                    print("ILLEGAL COMMAND")
                line = stdin.readline()
                command_split = line.split(" ")
                aCommand = command_split[0]
                if len(command_split) == 1:
                    aCommand = (command_split[0].split("\n"))[0]
        else:
            del final_database

