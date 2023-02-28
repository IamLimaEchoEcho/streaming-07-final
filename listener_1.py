"""
#
Lee Jones 
Module 07 - A7  

    This listener program will receive those transactions, test them against a current balance file (Current_Balance.csv) to alert low balance or overdrawn. 
    The emitter program will take a fictional transactional file (Transactions.csv) and read them at a rate of 1 per 5 seconds. 
     It will send a message that will contain the date, account number, and transaction amount.  
    

    
    Author:Lee Jones
    Date: February 23, 2023

"""

import pika
import sys
import time
import csv
import pandas as pd
from datetime import datetime
import smtplib 

#email variables: 
# creates SMTP session
s = smtplib.SMTP('smtp.gmail.com', 587)
# start TLS for security
s.starttls() 
# Authentication
s.login("*********@gmail.com", "************")




#load starting balances from Starting_Balance.csv
#input file
infile = 'Starting_Balance.csv'
infile_acct = "Account"
infile_sbal = "Starting Balance"

low_bal_amt = 100.00

tran_df = pd.read_csv(infile, usecols = [infile_acct, infile_sbal])
#print(tran_df)


            

#set queue names
queue1 = "tran_queue"

#create lists
lst_tran_time_temp = []


# callback

# define a callback function to be called when a transaction message is received
def tran_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Transaction Received: {body.decode()}")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    #ch.basic_ack(delivery_tag=method.delivery_tag)

    #save/split trans 
    tran_current_str = body.decode()
    tran_current_list = tran_current_str.split(", ")
    tran_date = tran_current_list[0]
    tran_acct = float(tran_current_list[1])
    tran_amt = float(tran_current_list[2])
    tran_merch = tran_current_list[3]


    acct_bal = 0

    if tran_date != '' and tran_acct != '' and tran_amt != '':
        for ind in tran_df.index: 
            test_acct = tran_df[infile_acct][ind]
            test_sbal = tran_df[infile_sbal][ind]
            
            if test_acct == tran_acct: 
                acct_bal = test_sbal
                acct_bal = acct_bal - tran_amt
                tran_df[infile_sbal][ind] = acct_bal

        if acct_bal <= low_bal_amt:
            if acct_bal < 0: 
                out_msg = "!!! Account Overdrawn!"
            else:
                out_msg = "!!  Low Balance Alert!"
        else:
            out_msg = ""

        if out_msg != "":
            print(f"{out_msg} - Acct:{tran_acct}, Tran Amt:{tran_amt}, Acct Bal:{acct_bal}")
            # message to be sent
            email_message = f"{out_msg} - Acct:{tran_acct}, Tran Amt:{tran_amt}, Acct Bal:{acct_bal}"
            # sending the mail
            s.sendmail("sender_email_id", "receiver_email_id", email_message)
        





# define a main function to run the program
def main(hn: str = "localhost", qn1: str = queue1):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn1, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn1, on_message_callback=tran_callback, auto_ack=True)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    #main("localhost", "task_queue2")
    main()

    
