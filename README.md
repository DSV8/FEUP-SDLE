# SDLE First Assignment

SDLE First Assignment of group T08G13.

Group members:

1. Diogo Viana (up202108803@fe.up.pt)
2. Gonçalo Martins (up202108707@fe.up.pt)
3. Maria Sofia Minnemann (up20200734@fe.up.pt)

## **How to Run**
To run the program, follow these steps:

### **1. Install Dependencies**
Ensure you have **Python** installed on your computer. Then, install the required libraries listed in the `requirements.txt` file. Additionally, install the **ZeroMQ library** for Python manually.

Run the following commands in the terminal:

```bash
pip install -r requirements.txt
```
### **2. Run the Cloud Side (Server)**

Start the cloud-side process by running `server.py`. Use the following commands in the terminal from the `src` folder:

```bash
python server.py
```
### **3. Run the Client Side**
After starting the server, run the client-side program in another terminal using `main.py`. Use the following commands in the terminal from the `src` folder:
```bash
python main.py
```