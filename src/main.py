from storage.shopping_list_manager import ShoppingListManager
from crdt.shopping_list import ShoppingList
from communication.client import Client
import sys
import keyboard


if sys.platform == "win32":
    import msvcrt
else:
    import termios
    import tty

def get_input_with_esc(prompt):
    print(prompt)
    print("(Press 'Esc' to go back)")
    input_text = ""

    if sys.platform == "win32":
        # Windows implementation using msvcrt
        while True:
            if msvcrt.kbhit():
                # Get the key pressed
                key = msvcrt.getch() 
                # Esc key -> return None
                if key == b'\x1b':  
                    print("\nReturning to the previous menu...")
                    return None
                # Enter key -> return the input text
                elif key == b'\r': 
                    print()  
                    return input_text.strip()
                # backspace key -> delete last character
                elif key == b'\b':  
                    if input_text:  
                        input_text = input_text[:-1]
                        print("\b \b", end="", flush=True)
                else:
                    # Append character to input text
                    input_text += key.decode('utf-8')
                    print(key.decode('utf-8'), end="", flush=True)

    else:
        def get_char():
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                char = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return char

        while True:
            char = get_char()
            if char == '\x1b':  # ESC key
                print("\nReturning to the previous menu...")
                return None
            elif char == '\r' or char == '\n':  # Enter key
                return input_text.strip()
            elif char == '\x7f' or char == '\b':  # Backspace
                
                input_text = input_text[:-1]
                print("\b \b", end="", flush=True)
            else:
                input_text += char
                sys.stdout.write(char)
                sys.stdout.flush()


def main():
    manager = ShoppingListManager()
    manager.load_from_json()
    client = Client()

    while (True):
        list_id = ''
        valid_list_id = False

        print("""
        Welcome!

        How would you like to proceed?

        [1] - Create new shopping list
        [2] - Edit existing shopping list
        [Q] - Quit
        """.replace('        ', ''))

        choice1 = input("Please choose one of the choices displayed above: ").upper()

        match choice1:
            case '1':
                list_id = manager.create_shopping_list()
                print(f"\nThis is your list ID: {list_id}")
                manager.save_to_json()
                client.create_shopping_list(list_id)
                valid_list_id = True
            case '2':
                list_id = input("\nType the ID of the list you want to edit: ")
                shopping_list = client.get_shopping_list(list_id)
                if(shopping_list == False):
                    print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                    manager.delete_shopping_list(list_id)
                    manager.save_to_json()
                    continue
                valid_list_id = True
            case 'Q':
                print("\nThank you, and come back soon!")
                break
            case _:
                print("\n\033[31;1mError:\033[0m Invalid input! Please, try again.")
                continue
    
        while ((True) and (valid_list_id)):
            print("""
            Which of the following operations would you like to perform?

            [1] - Add product to cart
            [2] - Remove product from cart
            [3] - Edit quantity of product in cart
            [4] - Purchase product in cart
            [5] - Check cart
            [6] - Delete list
            [Q] - Go back
            """.replace('            ', ''))

            choice2 = input("Please choose one of the options displayed above: ").upper()

            valid_product_id = False

            match choice2:
                case '1':     
                    while True:        
                        product_name = get_input_with_esc("\nWhat product would you like to add to your shopping list? ")
                        if product_name is None:  # 'Esc' was pressed
                            break
                        
                        if (product_name != ""):
                            product_name = product_name.lower()

                        manager.add_item_to_list(list_id, product_name)
                        shopping_list = client.write_shopping_list(manager.get_shopping_list(list_id), list_id)

                        if shopping_list == False:
                            print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                            manager.delete_shopping_list(list_id)
                            manager.save_to_json()
                            break
                        
                        # Overload merged list with local list
                        manager.shopping_lists[list_id] = shopping_list
                        manager.save_to_json()
                case '2':
                    while True:  # Infinite loop for retrying until Esc is pressed or valid input
                        product_name = get_input_with_esc("\nWhat product would you like to remove from your shopping list? ")
                        if product_name is None:  # 'Esc' was pressed
                            break
                        
                        if product_name != "":
                            product_name = product_name.lower()

                        # Try to find the product ID by name
                        product_id = manager.get_item_id_by_name(list_id, product_name)
                        if product_id is not None:
                            # Product ID found, proceed to remove the item
                            manager.remove_item_from_list(list_id, product_id)
                            product_name_cap = product_name.capitalize()
                            print(f"\n{product_name_cap} was removed from your shopping list successfully!")

                            shopping_list = client.write_shopping_list(manager.get_shopping_list(list_id), list_id)

                            if shopping_list == False:
                                print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                                manager.delete_shopping_list(list_id)
                                manager.save_to_json()
                                break

                            # Overload merged list with local list
                            manager.shopping_lists[list_id] = shopping_list
                            manager.save_to_json()
                            
                        else:
                            # Product not found in the list, display error and retry
                            product_name_cap = product_name.capitalize()
                            print(f"\n\033[31;1mError:\033[0m Item with name '{product_name_cap}' does not exist in list {list_id}.")

                case '3':
                    while True:  # Loop for selecting a valid product
                        product_name = get_input_with_esc("\n\nWhat product, from your shopping list, would you like to edit the quantity of? ")
                        if product_name is None:  # 'Esc' was pressed
                            print("\nAction canceled. Returning to the previous menu.")
                            break

                        if product_name != "":
                            product_name = product_name.lower()

                        # Check if the product exists in the list
                        product_id = manager.get_item_id_by_name(list_id, product_name)
                        if product_id is not None:
                            # Product ID is valid; proceed to quantity modification
                            while True:
                                print("""
                                Please choose how you want to proceed:

                                [1] - Increment quantity
                                [2] - Decrement quantity
                                [C] - Cancel
                                """.replace('                ', ''))

                                choice3 = input("Please choose one of the options displayed above: ").upper()

                                match choice3:
                                    case '1':
                                        increment_value_input = get_input_with_esc("\nEnter the amount to increment: ")
                                        if increment_value_input is None:  # 'Esc' was pressed
                                            print("\nAction canceled. Returning to the previous menu.")
                                            break
                                        
                                        try:
                                            increment_value = int(increment_value_input)  # Convert input to integer
                                            manager.increment_product_quantity(list_id, product_id, increment_value)
                                            print(f"\nSuccessfully incremented the quantity of '{product_name}' by {increment_value}.")
                                            shopping_list = client.write_shopping_list(manager.get_shopping_list(list_id), list_id)

                                            if not shopping_list:
                                                print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                                                manager.delete_shopping_list(list_id)
                                                manager.save_to_json()
                                                break

                                            # Overload merged list with local list
                                            manager.shopping_lists[list_id] = shopping_list
                                            manager.save_to_json()
                                            break  # Exit the loop after successful operation
                                        except ValueError:
                                            print("\n\033[31;1mError:\033[0m Invalid input! Please enter a valid number.")
                                            continue

                                    case '2':
                                        decrement_value_input = get_input_with_esc("\nEnter the amount to decrement: ")
                                        if decrement_value_input is None:  # 'Esc' was pressed
                                            print("\nAction canceled. Returning to the previous menu.")
                                            break

                                        try:
                                            decrement_value = int(decrement_value_input)  # Convert input to integer
                                            manager.decrement_product_quantity(list_id, product_id, decrement_value)
                                            print(f"\nSuccessfully decremented the quantity of '{product_name}' by {decrement_value}.")
                                            shopping_list = client.write_shopping_list(manager.get_shopping_list(list_id), list_id)

                                            if not shopping_list:
                                                print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                                                manager.delete_shopping_list(list_id)
                                                manager.save_to_json()
                                                break

                                            # Overload merged list with local list
                                            manager.shopping_lists[list_id] = shopping_list
                                            manager.save_to_json()
                                            break  # Exit the loop after successful operation
                                        except ValueError:
                                            print("\n\033[31;1mError:\033[0m Invalid input! Please enter a valid number.")
                                            continue
                                    case 'C':
                                        print("\nAction canceled. Returning to the previous menu.")
                                        break
                                    case _:
                                        print("\n\033[31;1mError:\033[0m Invalid input! Please, try again.")
                                        continue
                            break  # Exit the product selection loop after a successful edit
                        else:
                            # Invalid product name
                            product_name_cap = product_name.capitalize()
                            print(f"\n\033[31;1mError:\033[0m Item with name '{product_name_cap}' does not exist in list {list_id}.")

                case '4':
                    while True:
                            
                        product_name = get_input_with_esc("\nWhat product would you like to purchase from your shopping list? ")
                        if product_name is None:  # 'Esc' was pressed
                            break
                        
                        if product_name != "":
                            product_name = product_name.lower()

                        product_id = manager.get_item_id_by_name(list_id, product_name)

                        if (product_id != None):
                            valid_product_id = True
                            manager.acquire_item_from_list(list_id, product_id)
                            shopping_list = client.write_shopping_list(manager.get_shopping_list(list_id), list_id)

                            if shopping_list == False:
                                print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                                manager.delete_shopping_list(list_id)
                                manager.save_to_json()
                                break

                            # Overload merged list with local list
                            manager.shopping_lists[list_id] = shopping_list
                            manager.save_to_json()
                        
                        else:
                            product_name_cap = product_name.capitalize()
                            print(f"\n\033[31;1mError:\033[0m Item with name '{product_name}' does not exist in list {list_id}.")
                case '5':
                    shopping_list = client.get_shopping_list(list_id)
                    if(shopping_list == False):
                        print(f"\n\033[31;1mError:\033[0m List with ID '{list_id}' does not exist on the cloud.")
                        manager.delete_shopping_list(list_id)
                        manager.save_to_json()
                        break
                    elif(shopping_list == True):
                        manager.get_shopping_list(list_id).display_list()
                    else:
                        # Overload merged list with local list
                        manager.shopping_lists[list_id] = shopping_list
                        manager.save_to_json()
                        shopping_list.display_list()
                case '6':
                    manager.delete_shopping_list(list_id)
                    client.delete_shopping_list(list_id)
                    print("\nYour list has been successfully deleted!")
                    manager.save_to_json()
                    valid_list_id = False
                    break
                case 'Q':
                    break
                case _:
                    print("\n\033[31;1mError:\033[0m Invalid input! Please, try again.")
                    continue

    client.close_all_sockets()
    return
    

if __name__ == "__main__":
    main()