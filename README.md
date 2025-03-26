#I just wanted to check—is there a limit to how many images we can send in one API call to check if lights are on? For example:

Max images per request?

Any rate limits (calls per minute/hour)?

Does image size affect this?

Just want to make sure we stay within the limits. Let me know—thanks!
# de-use-cases

# Kafka REST API Examples

## Create a Topic

```
curl --location 'http://localhost:8082/v3/clusters/mRs5GWNUSmehOF71B9OjXQ/topics' \
--header 'Content-Type: application/json' \
--data '{
    "topic_name": "test-topic",
    "partitions_count": 1,
    "replication_factor": 1
}'
Produce a Message

Copy
curl --location 'http://localhost:8082/v3/clusters/mRs5GWNUSmehOF71B9OjXQ/topics/test-topic/records' \
--header 'Content-Type: application/json' \
--data '{
    "key": {
        "type": "STRING",
        "data": "txn12345"
    },
    "value": {
        "type": "JSON",
        "data": {
            "transaction_id": "txn12345",
            "store_id": "STR001",
            "pos_id": "POS789",
            "timestamp": "2024-03-22T14:35:00Z",
            "items": [
                {
                    "item_id": "ITEM001",
                    "name": "Laptop",
                    "quantity": 1,
                    "price": 999.99
                },
                {
                    "item_id": "ITEM002",
                    "name": "Mouse",
                    "quantity": 2,
                    "price": 19.99
                }
            ],
            "total_amount": 1039.97,
            "payment_method": "Credit Card",
            "customer_id": "CUST456"
        }
    }
}'
