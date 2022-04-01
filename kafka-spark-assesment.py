from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import uuid
import datetime
import json
import time



# kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic invoice-order



# for testing to check whether we have invoice generated or not.
# kafka-console-consumer --bootstrap-server localhost:9092 --topic invoice-order




TOPIC = "invoice-order"
SAMPLES = 1000
DELAY = 5 # seconds



order_id = random.randint(1, 100000) #string
item_id = random.randint(1, 100) #string
price = random.randint(1,50) #string
qty = random.randint(1,10) #string
states = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
stock_codes = ['85123A', '71053', '84406B', '84406G', '84406E']
customer_codes = [17850, 13047, 12583, 17850]



# broker runs in linux machine
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])




for i in range(SAMPLES):
state = random.choice(states)
invoice_no = str(uuid.uuid4().fields[-1])[:6]
invoice_no = int(invoice_no)

customer_code = random.choice(customer_codes)

current_time = datetime.datetime.now()

date = current_time.strftime('%m/%d/%Y %H:%M')

number_of_items = random.randint(3, 10)

for j in range(number_of_items):
# MM/dd/yyyy hh:mm
quantity = random.randint(1, 10)
unit_price = float(random.randint(1, 5))
stock_code = random.choice(stock_codes)
invoice = { "states" :state,
"Order ID": order_id,
"Item ID": item_id,
"Quantity": qty,
"Date": date,
"Price": price,
}
invoice_str = json.dumps(invoice)
print ("POS ", invoice_str)



key = invoice["states"]
producer.send(TOPIC, key=bytes(key,'utf-8'), value=bytes(invoice_str, 'utf-8'))

time.sleep(DELAY)