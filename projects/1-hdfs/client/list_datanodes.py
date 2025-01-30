import requests

# Define the Namenode API endpoint
NAMENODE_URL = "http://127.0.0.1:8000/datanodes"

def list_datanodes():
    try:
        response = requests.get(NAMENODE_URL)

        # Check if the request was successful
        if response.status_code == 200:
            datanodes = response.json().get("datanodes", [])
            
            if not datanodes:
                print("No datanodes found.")
                return

            print("\nList of available Datanodes:")
            for i, node in enumerate(datanodes, start=1):
                print(f"{i}. {node['host']}:{node['port']}")

        else:
            print(f"Failed to retrieve datanodes. Status Code: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Error connecting to namenode: {e}")

if __name__ == "__main__":
    list_datanodes()
