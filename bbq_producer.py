"""
    Chris Kollbaum 2/8/2023

    This code will take simulated real-time data sending temperature readings
    for a smoker and two foods in the smoker to rabbitmq where these readings
    will be processes and monitored by three consumers, where these will
    monitor and alert if a significant temperature change has occurred.
    

"""

import pika
import sys
import webbrowser
import csv
import socket
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer == True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_temp(host: str, queue_name: str, queue_name2: str, queue_name3: str, message: str):
    """
    Creates and sends a message to 3 queues each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue recieving data for the smoker temperature
        queue_name1 (str): the name of the queue recieving data for food A
        queue_name2 (str): the name of the queue recieving data for food B
        message (str): the message to be sent to the queue
    """
    host = "localhost"
    port = 9999
    address_tuple = (host, port)

    # use the socket constructor to create a socket object we'll call sock
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

    # read from a file to get some fake data
    input_file = open("smoker-temps.csv", "r")

    # create a csv reader for our comma delimited data
    reader = csv.reader(input_file, delimiter=",")


    for data_row in reader:
        # read a row from the file
        Time, Channel1, Channel2, Channel3 = data_row

        # sleep for a few seconds
        time.sleep(1)

        try:
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            #Deleting the three existing queues
            ch.queue_delete(queue_name)
            ch.queue_delete(queue_name2)
            ch.queue_delete(queue_name3)
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            ch.queue_declare(queue=queue_name, durable=True)
            ch.queue_declare(queue=queue_name2, durable=True)
            ch.queue_declare(queue=queue_name3, durable=True)
        

            try:
                Smoker = round(float(Channel1),2)
                # use an fstring to create a message from our data
                # notice the f before the opening quote for our string?
                smoker_data = f"[{Time}, {Smoker}]"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE = smoker_data.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE, address_tuple)
                ch.basic_publish(exchange="", routing_key=queue_name, body=MESSAGE)
                # print a message to the console for the user
                print(f" [x] Sent Smoker Temp {MESSAGE}")
            except ValueError:
                pass
            
            try:
                FoodA = round(float(Channel2),2)
                # use an fstring to create a message from our data
                # notice the f before the opening quote for our string?
                FoodA_data = f"[{Time}, {FoodA}]"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE2 = FoodA_data.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE2, address_tuple)
                ch.basic_publish(exchange="", routing_key=queue_name2, body=MESSAGE2)
                # print a message to the console for the user
                print(f" [x] Sent Food A Temp {MESSAGE2}")
            except ValueError:
                pass

            try:
                FoodB = round(float(Channel3),2)
                # use an fstring to create a message from our data
                # notice the f before the opening quote for our string?
                FoodB_data = f"[{Time}, {FoodB}]"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE3 = FoodB_data.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE3, address_tuple)
                ch.basic_publish(exchange="", routing_key=queue_name3, body=MESSAGE3)
                # print a message to the console for the user
                print(f" [x] Sent Food B Temp {MESSAGE3}")
            except ValueError:
                pass



        except pika.exceptions.AMQPConnectionError as e:
                print(f"Error: Connection to RabbitMQ server failed: {e}")
                sys.exit(1)

        finally:
            # close the connection to the server
            conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = False
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    message = " ".join(sys.argv[1:]) or '{MESSAGE}'
    # send the message to the queue
    send_temp("localhost","01-smoker", "02-Food-A", "02-Food-B", message)