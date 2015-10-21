__author__ = 'asifj'
## For only new messages

from kafka import KafkaConsumer

# To consume messages
consumer = KafkaConsumer('CLIEvent',
                         group_id='CLIEvent-grp',
                         bootstrap_servers=['172.22.147.242:9092', '172.22.147.232:9092', '172.22.147.243:9092'])
for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))
