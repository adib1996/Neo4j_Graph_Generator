import sys
import socket

def internet(host="8.8.8.8", port=53, timeout=3):
    """
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as ex:
        return False

def show_internet_connection():
    conn_succ = internet()
    if conn_succ == False:
        conn_status_string = 'Machine is not connected to internet. Please check the internet connectivity.'
        print ('\n%s\n'%conn_status_string)
        sys.exit()
