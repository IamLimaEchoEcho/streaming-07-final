"""
#
Lee Jones 
Module 07 - A7  

    This emitter program will take a fictional transactional file (Transactions.csv) and read them at a rate of 1 per 1 second. 
     It will send a message that will contain the date, account number, and transaction amount.  
    The listener program will receive those transactions, test them against a current balance file (Current_Balance.csv) to alert low balance or overdrawn. 

    
    Author:Lee Jones
    Date: February 23, 2023

"""

import pika
import sys
import webbrowser
import csv
import time 

#Delay in receiving each temp reading
sleep_secs = 1
#show the RabbitMQ admin website
show_offer = False
#input file
myfile = 'Transactions.csv'
infile_date = "Tran_Date"
infile_acct = "Account"
infile_amt = "Tran_Amt"
infile_merch = "Merchant"


#set queue names
queue1 = "tran_queue"

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer: 
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
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
    if show_offer: 
        offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    #message = " ".join(sys.argv[1:]) or "Second task....."
        
    with open(myfile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #hold the date/time
            t_date = row[infile_date]
            
            #prepare strings
            t_acct = row[infile_acct]
            t_amt = row[infile_amt]
            t_merch = row[infile_merch]
            
            #prepare binary message to stream
            tran_details = f"{t_date}, {t_acct}, {t_amt}, {t_merch}"
            tran_message_bin = tran_details.encode()

            #send message 
            if tran_details != '':
                send_message("localhost",queue1,tran_message_bin)
                message = queue1

            #wait 30 seconds to read next line
            time.sleep(sleep_secs)