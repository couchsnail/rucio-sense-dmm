import stomp
from dmm.utils.config import config_get

class MyListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print(f"Received an error {message}")

    def on_message(self, headers, message):
        print(f"Received message: {message}")

def connect_to_activemq():
    conn = stomp.Connection()
    conn.set_listener('', MyListener())
    conn.start()
    conn.connect('your_username', 'your_password', wait=True)
    return conn

def subscribe_to_queue(conn, queue_name):
    conn.subscribe(destination=queue_name, id=1, ack='auto')

def main():
    active_mq_host = 'localhost'  # Change this to your ActiveMQ broker's address
    active_mq_port = 61613  # Change this to your ActiveMQ broker's port
    queue_name = '/queue/your_queue'  # Change this to the desired queue

    try:
        conn = connect_to_activemq()
        subscribe_to_queue(conn, queue_name)

        while True:
            time.sleep(1)  # You can replace this with any processing logic

    except KeyboardInterrupt:
        print("Disconnecting from ActiveMQ")
        conn.disconnect()

if __name__ == '__main__':
    main()