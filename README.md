## Sherlock Firebase

### How to run locally

#### Set up envoriment

This requires Python 3.7 or over. It's recommended to run this in a virtual envoriment.

```
pip install requirements.txt
```

#### Configure key

Ask a fellow engineer to provide you with the encryption key. Then from the file `example_fernet_key.env` create `fernet_key.env` with the actual key. Then run:

```bash
source fernet_key.env
echo $_FERNET_SECRET_KEY
```

If everything works as intended you should see a 44 character long string.

#### Run tests

Run `python demo.py -h` to get the following instructions:

```
optional arguments:
  -h, --help            show this help message and exit
  -ost, --offer_search_trigger
                        Listen to Realtime Firebase Triggers and Publish to
                        Pubsub.
  -oe, --offer_enricher
                        Enrich offer output and update Firebase Realtime
                        Database.
  -pst, --product_search_trigger
                        Listen to Realtime Firebase Triggers on productSearch
                        and trigger to PubSub.
  -pspr, --product_search_publish_result
                        Publish to Realtime Firebase on productSearch.
  -ssfs, --sherlock_shopping_finish_signal
                        Consume finished message from Sherlock Google Shopping
                        and update Firebase.
  -dofd, --delete_old_firebase_data
                        Publish to Realtime Firebase on productSearch.

Argument Parser for the demo script.

```
